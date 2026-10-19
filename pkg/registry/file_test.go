package registry

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFileRegistry_Discover(t *testing.T) {
	path := filepath.Join(t.TempDir(), "address.json")
	if err := os.WriteFile(path, []byte(`{"Calculator":["a:1","b:2"]}`), 0o644); err != nil {
		t.Fatalf("WriteFile失败: %v", err)
	}

	reg := NewFileRegistry(path)
	addrs, ok := reg.Discover("Calculator")
	if !ok {
		t.Fatal("期望发现 Calculator")
	}
	if len(addrs) != 2 || addrs[0] != "a:1" || addrs[1] != "b:2" {
		t.Fatalf("地址不符合预期: %v", addrs)
	}

	if _, ok := reg.Discover("Missing"); ok {
		t.Fatal("不存在的服务应返回 false")
	}
}

func TestFileRegistry_BadFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "address.json")
	if err := os.WriteFile(path, []byte("{"), 0o644); err != nil {
		t.Fatalf("WriteFile失败: %v", err)
	}

	reg := NewFileRegistry(path)
	if _, ok := reg.Discover("Calculator"); ok {
		t.Fatal("非法 JSON 应返回 false")
	}

	if _, ok := NewFileRegistry(filepath.Join(t.TempDir(), "missing.json")).Discover("Calculator"); ok {
		t.Fatal("文件不存在应返回 false")
	}
}
