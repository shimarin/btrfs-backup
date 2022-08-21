all: btrfs-backup

btrfs-backup: btrfs-backup.cpp
	g++ -std=c++20 -o $@ -lbtrfsutil $<

clean:
	rm btrfs-backup

