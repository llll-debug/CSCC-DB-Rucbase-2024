# 设置要格式化的文件扩展名
EXTENSIONS=("c" "h" "cpp" "hpp")

# 查找并格式化文件
for ext in "${EXTENSIONS[@]}"; do
  find ./src -type f -name "*.$ext" -exec clang-format -i {} +
done

echo "Formatting complete!"
