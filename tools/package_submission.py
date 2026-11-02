"""Create the sole accepted submission format from student-owned sources."""
import argparse
import json
from pathlib import Path
import zipfile


def main():
    parser = argparse.ArgumentParser(description="打包 student/*.py 与提交描述；不包含运行数据")
    parser.add_argument("--output", type=Path, default=Path("submission.zip"))
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]  # package root; this script lives in tools/
    paths = sorted((root / "student").glob("*.py"))
    if any(path.is_symlink() for path in paths) or (root / "student").is_symlink():
        parser.error("student 文件不得使用符号链接")
    with zipfile.ZipFile(args.output, "x", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("submission.json", json.dumps({"format": "cs305-submission-v1",
            "client_entry": "student.client", "cdn_entry": "student.cdn"}))
        for path in paths:
            archive.write(path, "student/" + path.name)
    print(args.output)


if __name__ == "__main__":
    main()
