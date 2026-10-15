"""在终端里输入 TYPESAFE_API_KEY，写进 .env（权限 600）。输入不回显，也不会打印出来。"""

import getpass
import os

PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
NAME = "TYPESAFE_API_KEY"

for _ in range(3):
    key = getpass.getpass(f"{NAME}（输入不显示，粘贴后回车）: ").strip()
    if key and not any(c.isspace() for c in key):
        break
    print("没收到有效的 key（为空或含空白字符），再粘贴一次。")
else:
    raise SystemExit("没有保存。")

lines = []
if os.path.exists(PATH):
    with open(PATH) as f:
        lines = [line for line in f if not line.strip().startswith(f"{NAME}=")]
lines.append(f"{NAME}={key}\n")

fd = os.open(PATH, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
with os.fdopen(fd, "w") as f:
    f.writelines(lines)
os.chmod(PATH, 0o600)
print(f"已保存到 {PATH}（共 {len(key)} 个字符）。")
