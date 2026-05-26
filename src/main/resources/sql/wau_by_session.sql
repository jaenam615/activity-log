-- WAU 정의 (b): KST 기준 ISO 주별 DISTINCT session_id 수.
-- session_id 는 SessionAssigner 가 5분 이상 비활성 간격 규칙으로 생성한 값.
SELECT
  date_format(date_trunc('WEEK', event_date), 'yyyy-MM-dd') AS week_start_kst,
  COUNT(DISTINCT session_id)                                AS wau_sessions
FROM ${database}.${table}
WHERE event_date BETWEEN DATE '${from}' AND DATE '${to}'
GROUP BY date_trunc('WEEK', event_date)
ORDER BY week_start_kst;
