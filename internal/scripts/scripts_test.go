package scripts

import "testing"

func TestLoadParsesEveryEmbeddedCommand(t *testing.T) {
	reg, err := Load()
	if err != nil {
		t.Fatal(err)
	}

	if len(reg.scripts) < 50 {
		t.Fatalf("loaded %d scripts", len(reg.scripts))
	}

	for name, want := range map[string]int{"addStandardJob": 9, "moveToActive": 11, "moveToFinished": 14} {
		s := reg.Get(name)
		if s.Name() != name || s.NumKeys() != want || len(s.Hash()) != 40 {
			t.Errorf("%s: name=%q keys=%d hash=%q", name, s.Name(), s.NumKeys(), s.Hash())
		}
	}
}

func TestGetUnknownScriptPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected a panic")
		}
	}()

	Get("noSuchScript")
}

func TestCallArgsAndRunArgsKeyCheck(t *testing.T) {
	s := New("t", 2, "return 1")

	args := s.CallArgs(2)
	if len(args) != 3 || args[0] != "evalsha" || args[1] != s.Hash() || args[2] != "2" {
		t.Fatalf("CallArgs = %v", args)
	}

	if _, err := s.RunArgs(t.Context(), nil, append(args, "only-one-key")); err == nil {
		t.Fatal("RunArgs accepted too few keys")
	}

	if _, err := s.Run(t.Context(), nil, []string{"k"}); err == nil {
		t.Fatal("Run accepted too few keys")
	}
}

func TestBoxedBool(t *testing.T) {
	if BoxedBool(true) != "1" || BoxedBool(false) != "0" || ArgEmpty != "" {
		t.Fatal("unexpected boxed flags")
	}
}
