import os
import sys

print("===== ТЕСТ ТОКЕНА =====")
token = os.environ.get("TOKEN")
if token:
    # Ніколи не виводь токен повністю в логи!
    print(f"✅ Токен знайдено! Він починається з: {token[:10]}...")
    sys.exit(0)
else:
    print("❌ Змінна TOKEN відсутня або порожня!")
    sys.exit(1)
