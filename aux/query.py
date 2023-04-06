SILVER_QUERY = '''
SELECT
    CONCAT(vu.first_name, ' ', vu.last_name) AS full_name,
    vu.email AS email_user,
    CASE 
        WHEN vu.gender IS NULL THEN 'N/D' 
        ELSE vu.gender
    END AS gender_user,
    vu.username,
    vu.date_of_birth,
    vv.car_type,
    vv.color,
    vs.payment_method,
    vs.status,
    vv.dt_current_timestamp
FROM
    user AS vu
INNER JOIN
    vehicle AS vv
ON
    vu.user_id = vv.user_id
LEFT JOIN 
    subscription AS vs
ON
    vu.user_id = vs.user_id
'''

GOLD_QUERY = '''
SELECT
    full_name,
    email_user,
    FLOOR(DATEDIFF(NOW(), date_of_birth) / 365.25) AS age,
    CASE
        WHEN FLOOR(DATEDIFF(NOW(), date_of_birth) / 365.25) <= 21 THEN 'young' 
        WHEN FLOOR(DATEDIFF(NOW(), date_of_birth) / 365.25) <= 51 THEN 'adult'
        ELSE 'elder'
    END AS age_category,
    CASE 
        WHEN payment_method IS NULL THEN 'Payment not made'
        ELSE payment_method
    END AS payment_method,
    CASE 
        WHEN status IS NULL THEN 'N/D'
        ELSE status
    END AS status
FROM
    vw_user_payment
WHERE 
    status IN ('Blocked', 'Pending');
'''