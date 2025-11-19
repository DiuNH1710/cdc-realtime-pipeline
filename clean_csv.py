def clean_csv(input_path, output_path):
    with open(input_path, "r", encoding="utf-8") as infile, \
         open(output_path, "w", encoding="utf-8") as outfile:
        for line in infile:
            line = line.strip()

            # bỏ dấu " ở đầu/cuối dòng nếu có
            if line.startswith('"') and line.endswith('"'):
                line = line[1:-1]

            # bỏ các ;;; dư ở cuối dòng
            if line.endswith(";;;"):
                line = line[:-3]

            # thay thế chuỗi "" thành "
            line = line.replace('""', '"')

            # đổi dấu phẩy thành dấu |
            line = line.replace(",", "|")

            outfile.write(line + "\n")

# chạy thử
clean_csv("csv_files/tracking.csv", "csv_files/tracking_clean.csv")
