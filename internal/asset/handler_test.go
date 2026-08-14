package asset

import (
	"os"
	"path/filepath"
	"sort"
	"testing"
)

func TestResolveUploadRootSelectsInputDir(t *testing.T) {
	for name, props := range map[string]map[string]interface{}{
		"no root_path key":     {},
		"empty root_path":      {"root_path": ""},
		"root_path of a dot":   {"root_path": "."},
		"only unrelated keys":  {"dimensions": float64(2)},
		"root_path with peers": {"root_path": "", "metric": "cosine"},
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()

			root, declared, err := resolveUploadRoot(dir, props)
			if err != nil {
				t.Fatalf("got error %v, want nil", err)
			}
			if root != dir {
				t.Errorf("got root %s, want %s", root, dir)
			}
			if declared != "" {
				t.Errorf("got declared %q, want empty", declared)
			}
		})
	}
}

func TestResolveUploadRootSelectsDeclaredDirectory(t *testing.T) {
	dir := t.TempDir()
	bundle := filepath.Join(dir, "synth_12h.zarr")
	if err := os.Mkdir(bundle, 0o755); err != nil {
		t.Fatal(err)
	}

	root, declared, err := resolveUploadRoot(dir, map[string]interface{}{"root_path": "synth_12h.zarr"})
	if err != nil {
		t.Fatalf("got error %v, want nil", err)
	}
	if root != bundle {
		t.Errorf("got root %s, want %s", root, bundle)
	}
	if declared != "synth_12h.zarr" {
		t.Errorf("got declared %q, want synth_12h.zarr", declared)
	}
}

func TestResolveUploadRootRemovesTheKeyAndKeepsOthers(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "bundle"), 0o755); err != nil {
		t.Fatal(err)
	}
	props := map[string]interface{}{"root_path": "bundle", "sample_rate": float64(512)}

	if _, _, err := resolveUploadRoot(dir, props); err != nil {
		t.Fatalf("got error %v, want nil", err)
	}

	if _, ok := props["root_path"]; ok {
		t.Error("root_path reached the viewer asset, want it consumed")
	}
	if props["sample_rate"] != float64(512) {
		t.Errorf("got sample_rate %v, want 512", props["sample_rate"])
	}
}

func TestResolveUploadRootRejectsUnusableValues(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "zarr.json"), []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}

	for name, value := range map[string]interface{}{
		"a number":              float64(3),
		"a list":                []interface{}{"bundle"},
		"an absolute path":      "/etc",
		"a parent escape":       "../elsewhere",
		"a nested escape":       "bundle/../../elsewhere",
		"a missing directory":   "absent.zarr",
		"a file, not directory": "zarr.json",
	} {
		t.Run(name, func(t *testing.T) {
			props := map[string]interface{}{"root_path": value}

			if _, _, err := resolveUploadRoot(dir, props); err == nil {
				t.Fatal("got nil error, want a rejection")
			}
		})
	}
}

// The keys uploaded for a Zarr bundle must start at the bundle root, because
// the reader asks for zarr.json at the top of the asset prefix.
func TestDiscoveredFilesAreRelativeToTheDeclaredRoot(t *testing.T) {
	dir := t.TempDir()
	bundle := filepath.Join(dir, "synth_12h.zarr")
	if err := os.MkdirAll(filepath.Join(bundle, "0"), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"asset-properties.json"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("{}"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"zarr.json", filepath.Join("0", "zarr.json")} {
		if err := os.WriteFile(filepath.Join(bundle, name), []byte("{}"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	root, _, err := resolveUploadRoot(dir, map[string]interface{}{"root_path": "synth_12h.zarr"})
	if err != nil {
		t.Fatalf("got error %v, want nil", err)
	}
	files, err := discoverFiles(root)
	if err != nil {
		t.Fatalf("got error %v, want nil", err)
	}

	var keys []string
	for _, f := range files {
		rel, err := filepath.Rel(root, f)
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, filepath.ToSlash(rel))
	}
	sort.Strings(keys)

	want := []string{"0/zarr.json", "zarr.json"}
	if len(keys) != len(want) {
		t.Fatalf("got keys %v, want %v", keys, want)
	}
	for i := range want {
		if keys[i] != want[i] {
			t.Errorf("got keys %v, want %v", keys, want)
			break
		}
	}
}
