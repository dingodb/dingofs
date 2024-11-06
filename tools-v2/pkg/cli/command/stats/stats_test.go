package stats

import (
	"log"
	"testing"
)

func TestGetInode(t *testing.T) {
	mountPoint := "/"
	ino, _ := GetFileInode(mountPoint)
	if ino != 1 {
		log.Printf("path %s is not a mount point", mountPoint)
	}
}
