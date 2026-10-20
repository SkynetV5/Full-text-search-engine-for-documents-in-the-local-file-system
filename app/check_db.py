import ast
import sqlite3

conn = sqlite3.connect("../indeks.db")
cursor = conn.cursor()

#Download data
cursor.execute("SELECT doc_id, path, representation FROM dokumenty")
rows = cursor.fetchall()

print(f"📊 Liczba zindeksowanych dokumentów: {len(rows)}\n")

#Preview documents
for doc_id, path, representation in rows:
    print(f"📄 Dokument ID: {doc_id}")
    print(f"📁 Ścieżka: {path}")

    # change type data to dict
    tfidf_dict = ast.literal_eval(representation)

    print("🔑 Wagi TF-IDF słów:")
    for word, value in tfidf_dict.items():
        print(f"   • {word}: {value}")
    print("-" * 40)

conn.close()