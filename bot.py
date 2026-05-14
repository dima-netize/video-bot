import sys
print("Python version:", sys.version)

try:
    from telegram import Update
    print("✅ python-telegram-bot працює!")
except ImportError as e:
    print("❌ Помилка імпорту:", e)
    sys.exit(1)
