import json
import sys
import shutil
from pathlib import Path

def deep_update(original, updates):
    """
    Recursively update `original` dict with values from `updates` dict.
    """
    for key, value in updates.items():
        if isinstance(value, dict) and isinstance(original.get(key), dict):
            deep_update(original[key], value)
        else:
            original[key] = value
    return original

def main():
    if len(sys.argv) < 3:
        print(f"Usage: python {Path(__file__).name} <configs_folder> <overrides_file> [--dry-run] [--backup]")
        sys.exit(1)

    # Config Folder to update. E.g. "."
    configs_folder = Path(sys.argv[1]).resolve()
    # User config file that specifies fields to override
    user_override = Path(sys.argv[2]).resolve()

    dry_run = "--dry-run" in sys.argv #shows what files will be modified, but makes no changes
    make_backup = "--backup" in sys.argv #backs up all current files that are modified to a backup folder

    if not configs_folder.exists() or not configs_folder.is_dir():
        print(f"Error: {configs_folder} is not a valid folder.")
        sys.exit(1)
    if not user_override.exists() or not user_override.is_file():
        print(f"Error: {user_override} is not a valid file.")
        sys.exit(1)

    # Load overrides
    with open(user_override, "r") as f:
        overrides = json.load(f)

    # Prepare backup folder if needed
    backup_root = configs_folder / "backups"
    if make_backup and not dry_run:
        backup_root.mkdir(exist_ok=True)

    # Recursively process each JSON file in configs_folder
    for json_file in configs_folder.rglob("*.json"):
        #Skip updating user config itself
        if json_file.resolve() == user_override:
            continue

        if make_backup and not dry_run and backup_root in json_file.parents:
            continue  # Skip already-backed-up files

        # Open config file
        try:
            with open(json_file, "r") as f:
                config = json.load(f)
        except json.JSONDecodeError:
            print(f"Skipping {json_file} (invalid JSON).")
            continue

        # Update config file with overrides
        updated_config = deep_update(config.copy(), overrides)

        if dry_run:
            if config != updated_config:
                print(f"[DRY-RUN] Would update: {json_file}")
        else:
            # Backup if requested
            if make_backup:
                relative_path = json_file.relative_to(configs_folder)
                backup_path = backup_root / relative_path
                backup_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(json_file, backup_path)
                print(f"Backup saved: {backup_path}")

            # Save updated config
            with open(json_file, "w") as f:
                json.dump(updated_config, f, indent=2)
            print(f"Updated {json_file}")

    if dry_run:
        print("[OK] Dry run complete — no files were changed.")
    else:
        if make_backup:
            print(f"[OK] All configs updated. Backups stored in: {backup_root}")
        else:
            print("[OK] All configs updated (no backups made).")

if __name__ == "__main__":
    main()
