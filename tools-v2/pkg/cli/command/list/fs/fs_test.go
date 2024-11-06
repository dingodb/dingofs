package fs

import (
	"encoding/json"
	"fmt"
	"github.com/dingodb/dingofs/tools-v2/pkg/config"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetClusterFsInfo(t *testing.T) {
	// Set up the cobra command and its arguments
	caller := NewFsCommand()
	caller.Flag(config.CURVEFS_MDSADDR).Changed = true
	caller.Flag(config.CURVEFS_MDSADDR).Value.Set("172.20.7.232:16700,172.20.7.233:16700,172.20.7.234:16700")
	//caller.SetArgs([]string{"--mdsaddr", "172.20.7.232:16700,172.20.7.233:16700,172.20.7.234:16700"})
	// Call the function being tested
	listCluster, err := GetClusterFsInfo(caller)
	if err.Code != 0 {
		t.Fatalf("Expected no error from GetClusterFsInfo, but got: %v", err)
	}

	if listCluster == nil {
		t.Fatal("Expected listCluster to be non-nil, but it was nil")
	}

	// Call GetFsInfo and check for nil
	fsInfo := listCluster.GetFsInfo()
	if fsInfo == nil {
		t.Fatal("Expected listCluster.GetFsInfo() to return non-nil, but it was nil")
	}

	// Marshal fsInfo to JSON
	fsInfoJson, errFormat := json.MarshalIndent(fsInfo, "", "  ")
	if errFormat != nil {
		t.Fatalf("Failed to marshal fsInfo to JSON: %v", errFormat)
	}

	// Print fsInfo in JSON format
	fmt.Println("Filesystem Info:", string(fsInfoJson))
	assert.NotNil(t, fsInfo, "Expected fsInfo to be non-nil")

	// get mount point by fsName
	for _, fs := range fsInfo {
		mountpoints := fs.GetMountpoints()
		fmt.Printf("fsId:[%d], fsName:[%s], Mount Point:\n", fs.GetFsId(), fs.GetFsName())
		for _, mountpoint := range mountpoints {
			fmt.Printf("%s\n", mountpoint.GetPath())
		}
	}

}
