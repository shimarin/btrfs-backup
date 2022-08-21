// btrfs-backup.cpp
// Written by Tomoatsu Shimada
// https://twitter.com/shimariso

#include <unistd.h>

#include <iostream>
#include <filesystem>

#include <btrfsutil.h>

std::string to_string(uint8_t uuid[16])
{
    char buf[48];
    sprintf(buf, "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
        (int)uuid[0],(int)uuid[1],(int)uuid[2],(int)uuid[3],(int)uuid[4],(int)uuid[5],(int)uuid[6],(int)uuid[7],
        (int)uuid[8],(int)uuid[9],(int)uuid[10],(int)uuid[11],(int)uuid[12],(int)uuid[13],(int)uuid[14],(int)uuid[15]);
    return buf;
}

int to_DOW(const std::filesystem::path& subvol_path)
{
    struct btrfs_util_subvolume_info subvol;
    auto rst = btrfs_util_subvolume_info(subvol_path.c_str(), 0, &subvol);
    if (rst != BTRFS_UTIL_OK) {
        throw std::runtime_error("Inspecting subvolume " + subvol_path.string() + " failed(" + btrfs_util_strerror(rst) + ")");
    }

    return localtime(&subvol.otime.tv_sec)->tm_wday;
}

static const char* DOWSTR[] = {"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"};

void delete_subvolume_if_exists(const std::filesystem::path& subvol)
{
    btrfs_util_delete_subvolume(subvol.c_str(), BTRFS_UTIL_DELETE_SUBVOLUME_RECURSIVE);
    if (std::filesystem::exists(subvol)) {
        throw std::runtime_error("Subvolume " + subvol.string() + " cannot be deleted(not a subvolume?)");
    }
}

void create_readonly_snapshot(const std::filesystem::path& src, const std::filesystem::path& snapshot)
{
    auto rst = btrfs_util_create_snapshot(src.c_str(), snapshot.c_str(), BTRFS_UTIL_CREATE_SNAPSHOT_READ_ONLY, NULL, NULL);
    if (rst != BTRFS_UTIL_OK) {
        throw std::runtime_error("Creating readonly snapshot " + snapshot.string() + " failed(" + btrfs_util_strerror(rst) + ")");
    }
}

void perform_incremental_backup(const std::filesystem::path& src, const std::filesystem::path& dst)
{
    auto dst_head = dst / "head";
    auto dst_head_new = dst / "head.new";
    delete_subvolume_if_exists(dst_head_new);
    auto src_head = src / ".snapshots/head";
    auto src_head_new = src / ".snapshots/head.new";
    delete_subvolume_if_exists(src_head_new);
    create_readonly_snapshot(src.c_str(), src_head_new.c_str());
    sync();
    // TODO: escape paths
    auto cmd = "btrfs send -p " + src_head.string() + " " + src_head_new.string() + " | btrfs receive " + dst.string();
    std::cout << cmd << std::endl;
    auto rst = system(cmd.c_str());
    if (!WIFEXITED(rst) || WEXITSTATUS(rst) != 0) {
        throw std::runtime_error("Error in btrfs command");
    }

    auto dow = DOWSTR[to_DOW(src_head)];
    auto src_dow = src / ".snapshots" / dow;
    auto dst_dow = dst / dow;
    delete_subvolume_if_exists(src_dow);
    delete_subvolume_if_exists(dst_dow);
    sync();
    std::filesystem::rename(src_head, src_dow);
    std::filesystem::rename(dst_head, dst_dow);

    std::filesystem::rename(src_head_new, src_head);
    std::filesystem::rename(dst_head_new, dst_head);
}

void perform_full_backup(const std::filesystem::path& src, const std::filesystem::path& dst)
{
    auto src_head = src / ".snapshots/head";
    if (btrfs_util_is_subvolume(src_head.c_str()) == BTRFS_UTIL_OK) {
        auto src_dow = src / ".snapshots" / DOWSTR[to_DOW(src_head)];
        delete_subvolume_if_exists(src_dow);
        std::filesystem::rename(src_head, src_dow);
    }

    auto dst_head = dst / "head";
    if (btrfs_util_is_subvolume(dst_head.c_str()) == BTRFS_UTIL_OK) {
        auto dst_dow = dst / DOWSTR[to_DOW(dst_head)];
        delete_subvolume_if_exists(dst_dow);
        std::filesystem::rename(dst_head, dst_dow);
    }
    sync();
    std::filesystem::create_directory(src / ".snapshots"); // just returns false if already exists
    create_readonly_snapshot(src, src_head);
    sync();
    // TODO: escape paths
    auto cmd = "btrfs send " + src_head.string() + " | btrfs receive " + dst.string();
    std::cout << cmd << std::endl;
    auto rst = system(cmd.c_str());
    if (!WIFEXITED(rst) || WEXITSTATUS(rst) != 0) {
        throw std::runtime_error("Error in btrfs command");
    }
}

void must_be_a_subvolume(const std::filesystem::path& path)
{
    auto rst = btrfs_util_is_subvolume(path.c_str());
    if (rst != BTRFS_UTIL_OK) {
        throw std::runtime_error(path.string() + " is not a BTRFS subvolume(" + btrfs_util_strerror(rst) + ")");
    }
}

int main(int argc, char* argv[])
{
    if (argc != 3) {
        std::cerr << "Usage: " << std::endl;
        std::cerr << argv[0] << " <src subvolume> <dst subvolume>" << std::endl;
        return -1;
    }

    //else
    const std::filesystem::path src(argv[1]), dst(argv[2]);

    try {
        must_be_a_subvolume(src);
        must_be_a_subvolume(dst);

        bool incremental_backup_possible = false;
        struct btrfs_util_subvolume_info subvol;
        if (btrfs_util_subvolume_info((src / ".snapshots/head").c_str(), 0, &subvol) == BTRFS_UTIL_OK) {
            auto src_uuid = to_string(subvol.uuid);
            if (btrfs_util_subvolume_info((dst / "head").c_str(), 0, &subvol) == BTRFS_UTIL_OK) {
                auto received_uuid = to_string(subvol.received_uuid);
                incremental_backup_possible = src_uuid == received_uuid;
            }
        }

        if (incremental_backup_possible) {
            std::cout << "Incremental backup" << std::endl;
            perform_incremental_backup(src, dst);
            std::cout << "Done." << std::endl;
        } else {
            std::cout << "Full backup" << std::endl;
            perform_full_backup(src, dst);
            std::cout << "Done." << std::endl;
        }
    }
    catch (const std::runtime_error& err) {
        std::cerr << err.what() << std::endl;
        return 1;
    }

    return 0;
}