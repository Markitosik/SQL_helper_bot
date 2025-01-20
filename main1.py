import re
import logging
from aiogram import Bot, types
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram import Dispatcher
from aiogram.filters import Command
import asyncio


from config import API_TOKEN

# Инициализация логирования
logging.basicConfig(level=logging.INFO)

# Инициализация бота и диспетчера
bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dispatcher = Dispatcher()

# Константы
VALID_NAME_REGEX = r'^[a-zA-Z_][a-zA-Z0-9_]*$'
SQL_RESERVED_KEYWORDS = {
    "SELECT", "FROM", "WHERE", "INSERT", "UPDATE", "DELETE", "JOIN", "GROUP", "ORDER", "BY",
    "HAVING", "IN", "IS", "NULL", "AND", "OR", "NOT", "LIKE", "BETWEEN", "EXISTS", "DISTINCT",
    "CASE", "WHEN", "THEN", "ELSE", "END"
}


def is_valid_name(name: str) -> bool:
    """Проверка валидности имени таблицы или столбца."""
    return bool(re.match(VALID_NAME_REGEX, name)) and name.upper() not in SQL_RESERVED_KEYWORDS


def parse_table_line(line: str) -> dict:
    """Парсинг строки с описанием таблицы."""
    table_info = line.strip().split(" : ")
    table_name_parts = table_info[0].split(" ")

    table_name = table_name_parts[0]
    alias = None
    if "-" in table_name_parts:
        alias_index = table_name_parts.index("-")
        alias = table_name_parts[alias_index + 1] if len(table_name_parts) > alias_index + 1 else None

    columns = table_info[1].split() if len(table_info) > 1 and table_info[1] else ["*"]
    join_condition = table_info[2].strip() if len(table_info) > 2 else None

    return {
        "name": table_name,
        "alias": alias,
        "columns": columns,
        "join_condition": join_condition
    }


def generate_sql(query: str) -> str:
    """Генерация SQL-запроса с сохранением алгоритма."""
    lines = query.strip().split("\n")
    if not lines:
        return "Ошибка: Запрос не должен быть пустым."

    tables = []
    for line in lines:
        table = parse_table_line(line)

        if not is_valid_name(table["name"]):
            return f"Ошибка: Имя таблицы '{table['name']}' недопустимо или зарезервировано."

        if table["alias"] and not is_valid_name(table["alias"]):
            return f"Ошибка: Псевдоним '{table['alias']}' недопустим или зарезервирован."

        if not all(is_valid_name(col) for col in table["columns"] if col != "*"):
            return "Ошибка: Одно из имен столбцов недопустимо или зарезервировано."

        tables.append(table)

    if any(i > 0 and not t["join_condition"] for i, t in enumerate(tables)):
        return "Ошибка: Все таблицы, кроме первой, должны иметь условие соединения."

    select_clause = "SELECT"
    select_columns = []
    from_clause = ""
    join_clauses = []

    for i, table in enumerate(tables):
        if i == 0:
            from_clause = (
                f"FROM {table['name']} AS {table['alias']}" if table["alias"] else f"FROM {table['name']}"
            )
        else:
            join_clauses.append(
                f"\nJOIN {table['name']} AS {table['alias']} ON {table['join_condition']}" if table["alias"] else
                f"\nJOIN {table['name']} ON {table['join_condition']}"
            )

        formatted_columns = [
            f"{table['alias'] or table['name']}.{col}" for col in table["columns"]
        ]
        select_columns.extend(formatted_columns)

    sql_query = f"{select_clause} {', '.join(select_columns)} {from_clause}{''.join(join_clauses)};"
    return sql_query


# Хэндлеры
async def send_welcome(message: types.Message):
    """Приветственное сообщение."""
    await message.answer(
        "Привет! Чтобы создать SQL-запрос, используй следующий формат:\n\n"
        "таблица \\[ - псевдоним ] : \\[ столбцы ] : \\[ условие соединения JOIN]\n\n"
        "🔹 *Важно:*\n"
        "- У первой таблицы *не должно быть условия соединения*.\n"
        "- Условия соединения задаются для всех последующих таблиц.\n\n"
        "Пример:\n"
        "`customers - c : name age : c.id = orders.customer_id`\n\n"
        "📝 *Пример полного ввода:*\n"
        "`customers - c : name age\norders - o : order_date : c.id = o.customer_id`",
        parse_mode="Markdown"
    )


async def process_sql_query(message: types.Message):
    """Обработка пользовательского ввода и генерация SQL-запроса."""
    query = message.text.strip()
    sql_query = generate_sql(query)
    await message.answer(f"Генерируемый SQL запрос:\n```sql\n{sql_query}```", parse_mode="Markdown")


# Основная функция
async def main():
    dispatcher.message.register(send_welcome, Command("start"))
    dispatcher.message.register(process_sql_query)
    await dispatcher.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
