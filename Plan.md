## Запросы:

| Запросы                 | Приоритетность | Готовность | Статус |
|-------------------------|----------------|------------|-------|
| 1. Result               |                | ✅         | ✅     |
| 2. Values Scan          |                | ✅          | ✅     |
| 3. Function Scan        |                | ✅         | ✅     |
| 4. Incremental Sort     |                | ✅           | ✅     |
| 5. Unique               |                | ✅          | ✅     |
| 6. Append               |                | ✅          | ✅     |
| 7. Merge Append         |                | ✅          | ✅     |
| 8. Subquery Scan        |                | ✅          | ✅     |
| 9. HashSetOp            |                | X          | X     |
| 10. SetOp               |                | ✅         | ✅     |
| 11. Materialize         |                | ✅          | ✅     |
| 12. Memoize             |                | X          | X     |
| 13. Group               |                |✅          | ✅     |
| 14. Aggregate           |                | ✅          | ✅     |
| 15. GroupAggregate      |                | ✅          | ✅     |
| 16. HashAggregate       |                | ✅          | ✅     |
| 17. MixedAggregate      |                | X          | X     |
| 18. WindowAgg           |                | ✅           | ✅     |
| 19. Parallel Seq Scan   | X              | X          | X     |
| 20. Gather              | X              | X          | X     |
| 21. Finalize Aggregate  | X              | X          | X     |
| 22. Gather Merge        | X              | X          | X     |
| 23. Parallel Append     | X              | X          | X     |
| 24. Parallel Hash       | X              | X          | X     |
| 25. Parallel Hash Join  | X              | X          | X     |
| 26. CTE Scan            |                | ✅           | ✅     |
| 27. WorkTable Scan      |                | ✅           | ✅     |
| 28. Nested Loop         |                | ✅          | ✅     |
| 29. Recursive Union     |                | ✅           | ✅      |
| 30. ProjectSet          |                | ✅          | ✅     |
| 31. LockRows            |                | ✅         | ✅    |
| 32. Sample Scan         |                | ✅           | ✅     |
| 33. Table Function Scan |                | X          | X     |
| 34. Foreign Scan        |                | X          | X     |
| 35. Tid Scan            |                | ✅           | ✅     |
| 36. Insert              | X              | X          | X     |
| 37. Update              | X              | X          | X     |
| 38. Delete              | X              | X          | X     |
| 39. Merge               |                | X          | X     |
| 40. Semi Join           |                | X          | X     |
| 41. Anti Join           |                | X          | X     |
| 42. SubPlan             |                | X          | X     |
| 43. Hash Join           |                | ✅          | ✅     |
| 44. Sort                |                | ✅          | ✅     |
| 45. Merge Join          |                | ✅          | ✅     |

## Действия:

| Действие                                                                      | Статус |
|-------------------------------------------------------------------------------|---|
| Запуск всех тестов                                                            | ✅ |
| Проверка на корректность плана <br/>(создать парсер сообщения в формате json) | ✅  |
| Сделать удобное лог                                                           | ✅ |
| Выводить время выполнения 1 запроса 1 процессом                               | ✅ |
| Разделить таблицы на типы по размеру (малые, большие)                         | ✅ |
| Создать 4 вида запуска на каждый тест в зависимости от размера таблицы        | ✅ |
| Исследовать параллельное выполнение запросов                                  | ✅ |
| Сделать отчет в конце запроса по некольким запросам одного плана              |  ✅ |
| Добавить удаление таблиц на каждую группу таблиц по размеру                   | ✅ |
