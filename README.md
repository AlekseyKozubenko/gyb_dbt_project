# gyb_dbt_project
Тестове завдання по використанню dbt на базі даних PostgresSQL

Для виконання були встановлені пакети "dbt" та "PostgresSQL", свторена база даних "db"

В БД були завантажені дані із CSV файла, який попередньо був конвертований із XLSX

Були виконані наступні завдання:

Використовуючи dbt (Data Building Tool) створити вітрину даних, що буде представляти собою таблицю fct_sales, яка міститиме інформацію про кожну унікальну продажу, включаючи інформацію про: 
-	Назву проданого продукту
-	Список імен продавців, які були залучені в цій продажі
-	Країна
-	Кампанія
-	Джерело продажі (колонка Source)
-	Дохід компанії від продажі, враховуючи повторні оплати (rebill) та повернення коштів
-	Дохід компанії тільки від ребілів
-	К-ть ребілів
-	Сума знижки
-	Сума повернених коштів
-	Дата повернення коштів (3 поля, для трьох тайм зон: Kyiv, UTC, New York)
-	Різниця днів від дати повернення коштів та датою покупки
-	Дата продажі (3 поля, для трьох тайм зон: Kyiv, UTC, New York)

Маючи готову таблицю, в папці Analyses дайте відповіді на такі запитання:
-	розрахуйте відсоткове зростання доходу від місяця до місяця.
-	Для кожного агента визначте його середній дохід, к-ть продажів, а також середнє значення запропонованих знижок на кожну покупку. Посортуйте по загальній сумі дохідності, та розставте рангові місця: 1, 2, 3, …, n 
-	визначте агентів, які надають знижки вище загального середнього рівня.

