-- WAU 정의 (a): KST 기준 ISO 주(월요일 시작)별 DISTINCT user_id 수.
-- ${database}, ${table}, ${from}, ${to} 는 호출부에서 치환하여 사용.
SELECT
  date_format(date_trunc('WEEK', event_date), 'yyyy-MM-dd') AS week_start_kst,
  COUNT(DISTINCT user_id)                                   AS wau_users
FROM ${database}.${table}
WHERE event_date BETWEEN DATE '${from}' AND DATE '${to}'
GROUP BY date_trunc('WEEK', event_date)
ORDER BY week_start_kst;
