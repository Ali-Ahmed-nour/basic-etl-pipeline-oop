# 🛠️ Basic ETL Pipeline (Object-Oriented Version)

This is a basic educational ETL project based on IBM’s [“Python Project for Data Engineering”](https://www.coursera.org/learn/python-project-for-data-engineering) course on Coursera.

> 🔍 **Note:** The original course project was written using procedural programming.  
> I refactored the code into an **Object-Oriented Python class** to practice modularity, structure, and reusability.

---

## 📌 What This Project Does

This pipeline reads structured data from different file formats in a local folder:
- CSV
- JSON (line-delimited)
- XML

It then:
1. Appends all the data vertically (row-wise)
2. Converts height from inches to meters and weight from pounds to kilograms
3. Exports the cleaned, combined result to a single CSV file with a timestamped name
4. Logs all ETL steps using Python’s built-in `logging` module

---

## 📂 Project Structure

```
basic-etl-pipeline-oop/
├── etl_append_pipeline.ipynb       ← Main Jupyter Notebook (ETL logic)
├── source/                         ← Folder with input CSV / JSON / XML files
├── ETL_Result/                     ← Contains results + generated log file
├── README.md                       ← You are here
```

> ✔️ Log file and output CSV are automatically created in the `ETL_Result` folder with timestamps.

---

## 🚀 How It Works

The project uses:
- `pandas` for data manipulation
- `xml.etree.ElementTree` to parse XML
- `glob` to read files by extension
- `os` for path handling
- `logging` for logging progress

The main class `ETLAppend` handles the full pipeline and runs automatically upon object creation.

---

## 🧪 Example Use

After adjusting paths and expected column names at the top of the notebook:

```python
etl = ETLAppend(source_path, target_path, expected_columns)
```

This will:
- Extract data from all `.csv`, `.json`, and `.xml` files in `source_path`
- Transform height/weight fields
- Save result in `ETL_Result/` as `appended_output_YYYY-MM-DD_HH-MM.csv`
- Write logs in `etl_append_log.log`

---

## 🎯 Learning Objectives

This project helped me practice:

- ✅ ETL concepts
- ✅ Clean code structure using OOP
- ✅ Working with multiple file formats
- ✅ Logging and debugging in Python
- ✅ Uploading and managing GitHub projects

---

## 📚 Based On

Course: [Python Project for Data Engineering – IBM on Coursera](https://www.coursera.org/learn/python-project-for-data-engineering)

---

## 🤝 Contributions

This project is intended for **learning and personal improvement only**.  
Feel free to fork or reuse the code for your own experiments.

---

## 📥 Contact

Feel free to reach out to me via [LinkedIn](https://www.linkedin.com/in/ali-ahmed-nour/) or check more projects on my [GitHub](https://github.com/Ali-Ahmed-nour).
