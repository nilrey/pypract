import tkinter as tk

def press(key):
    entry.insert(tk.END, key)

def clear():
    entry.delete(0, tk.END)

def calculate():
    try:
        result = eval(entry.get())
        entry.delete(0, tk.END)
        entry.insert(tk.END, str(result))
    except:
        entry.delete(0, tk.END)
        entry.insert(tk.END, "Ошибка")

root = tk.Tk()
root.title("Калькулятор")

entry = tk.Entry(root, width=20, font=("Arial", 16))
entry.grid(row=0, column=0, columnspan=4)

buttons = [
    ("7", 1, 0), ("8", 1, 1), ("9", 1, 2), ("/", 1, 3),
    ("4", 2, 0), ("5", 2, 1), ("6", 2, 2), ("*", 2, 3),
    ("1", 3, 0), ("2", 3, 1), ("3", 3, 2), ("-", 3, 3),
    ("0", 4, 0), (".", 4, 1), ("=", 4, 2), ("+", 4, 3),
]

for text, row, col in buttons:
    action = lambda x=text: press(x) if x != "=" else calculate()
    tk.Button(root, text=text, command=action, font=("Arial", 14)).grid(row=row, column=col, sticky="nsew")

tk.Button(root, text="C", command=clear, font=("Arial", 14)).grid(row=5, column=0, columnspan=4, sticky="nsew")

root.mainloop()
