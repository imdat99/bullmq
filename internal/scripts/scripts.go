// Package scripts embeds the BullMQ Lua commands and runs them on Redis.
//
// The commands (includes already resolved) are shared with the Node.js,
// Python, Elixir and Rust implementations. File names are
// `{name}-{numKeys}.lua`.
package scripts

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"path"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"
)

//go:embed commands/*.lua
var commandsFS embed.FS

// Script is one BullMQ Lua command. It is immutable and safe for concurrent
// use.
type Script struct {
	name    string
	numKeys int
	script  *redis.Script
	// Pre-boxed EVALSHA/EVAL arguments for RunArgs: boxing them once avoids
	// an allocation per argument on every call.
	shaArg, srcArg, numKeysArg any
}

// New returns a script named name taking numKeys keys.
func New(name string, numKeys int, src string) *Script {
	script := redis.NewScript(src)

	return &Script{
		name: name, numKeys: numKeys, script: script,
		shaArg: script.Hash(), srcArg: src, numKeysArg: strconv.Itoa(numKeys),
	}
}

// Name returns the command name (the file name without the key count).
func (s *Script) Name() string { return s.name }

// NumKeys returns the number of KEYS the script expects.
func (s *Script) NumKeys() int { return s.numKeys }

// Hash returns the SHA1 digest used by EVALSHA.
func (s *Script) Hash() string { return s.script.Hash() }

// Registry holds every embedded script. It is immutable after construction
// and safe for concurrent use.
type Registry struct {
	scripts map[string]*Script
}

// Load parses the embedded commands. It only fails if the binary was built
// with a malformed commands directory.
func Load() (*Registry, error) {
	entries, err := commandsFS.ReadDir("commands")
	if err != nil {
		return nil, fmt.Errorf("bullmq: read embedded commands: %w", err)
	}

	reg := &Registry{scripts: make(map[string]*Script, len(entries))}
	for _, e := range entries {
		base := strings.TrimSuffix(e.Name(), ".lua")

		dash := strings.LastIndexByte(base, '-')
		if dash < 0 {
			return nil, fmt.Errorf("bullmq: malformed command file name %q", e.Name())
		}

		numKeys, err := strconv.Atoi(base[dash+1:])
		if err != nil {
			return nil, fmt.Errorf("bullmq: malformed command file name %q: %w", e.Name(), err)
		}

		src, err := commandsFS.ReadFile(path.Join("commands", e.Name()))
		if err != nil {
			return nil, fmt.Errorf("bullmq: read command %q: %w", e.Name(), err)
		}

		name := base[:dash]
		reg.scripts[name] = New(name, numKeys, string(src))
	}

	return reg, nil
}

var defaultRegistry = func() *Registry {
	reg, err := Load()
	if err != nil {
		panic(err) // the embedded files are part of the build; this is a build bug
	}

	return reg
}()

// Get returns the embedded script name. It panics on an unknown name: all
// names are compile-time constants of the bullmq package.
func Get(name string) *Script { return defaultRegistry.Get(name) }

// Get returns the script name; it panics on an unknown name.
func (r *Registry) Get(name string) *Script {
	s, ok := r.scripts[name]
	if !ok {
		panic("bullmq: unknown script " + name)
	}

	return s
}

// Run executes the script with EVALSHA, falling back to EVAL on NOSCRIPT
// (which also covers a restarted Redis with an empty script cache). A nil Lua
// reply is returned as (nil, nil).
func (s *Script) Run(ctx context.Context, c redis.Scripter, keys []string, args ...any) (any, error) {
	if len(keys) != s.numKeys {
		return nil, fmt.Errorf("bullmq: script %s expects %d keys, got %d", s.name, s.numKeys, len(keys))
	}

	res, err := s.script.Run(ctx, c, keys, args...).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}

	if err != nil {
		return nil, fmt.Errorf("bullmq: %s: %w", s.name, err)
	}

	return res, nil
}

// EvalSha queues an EVALSHA of the script, typically on a pipeline (which has
// no NOSCRIPT fallback: [Script.Load] it first).
func (s *Script) EvalSha(ctx context.Context, c redis.Scripter, keys []string, args ...any) *redis.Cmd {
	return s.script.EvalSha(ctx, c, keys, args...)
}

// Load registers the script on the server (SCRIPT LOAD).
func (s *Script) Load(ctx context.Context, c redis.Scripter) error {
	return s.script.Load(ctx, c).Err()
}

// Pre-boxed command names and flags (see Script.shaArg).
var (
	argEvalSha any = "evalsha"
	argEval    any = "eval"
	argOne     any = "1"
	argZero    any = "0"

	// ArgEmpty is a pre-boxed empty string argument.
	ArgEmpty any = ""
)

// BoxedBool returns the pre-boxed "1" or "0" script flag.
func BoxedBool(b bool) any {
	if b {
		return argOne
	}

	return argZero
}

// CallArgs starts an argument list for [Script.RunArgs] with room for n keys
// and arguments.
func (s *Script) CallArgs(n int) []any {
	args := make([]any, 3, 3+n)
	args[0], args[1], args[2] = argEvalSha, s.shaArg, s.numKeysArg

	return args
}

// RunArgs is the allocation-lean variant of [Script.Run] used on the hot
// paths: args comes from [Script.CallArgs] followed by the keys (typically
// pre-boxed) and the arguments. It sends EVALSHA, falling back to EVAL on
// NOSCRIPT. go-redis routes "evalsha" by its first key on a cluster.
func (s *Script) RunArgs(ctx context.Context, c redis.UniversalClient, args []any) (any, error) {
	if len(args) < 3+s.numKeys {
		return nil, fmt.Errorf("bullmq: script %s expects %d keys, got %d args", s.name, s.numKeys, len(args)-3)
	}

	res, err := c.Do(ctx, args...).Result()
	if err != nil && isNoScript(err) {
		args[0], args[1] = argEval, s.srcArg
		res, err = c.Do(ctx, args...).Result()
	}

	if errors.Is(err, redis.Nil) {
		return nil, nil
	}

	if err != nil {
		return nil, fmt.Errorf("bullmq: %s: %w", s.name, err)
	}

	return res, nil
}

func isNoScript(err error) bool {
	return errors.Is(err, redis.ErrNoScript) || redis.HasErrorPrefix(err, "NOSCRIPT")
}
