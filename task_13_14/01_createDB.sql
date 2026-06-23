CREATE TABLE IF NOT EXISTS transactions (
    msno Utf8 NOT NULL,
    payment_method_id Int32,
    payment_plan_days Int32,
    plan_list_price Int32,
    actual_amount_paid Int32,
    is_auto_renew Int32,
    transaction_date Int32,
    membership_expire_date Int32,
    is_cancel Int32,
    PRIMARY KEY (msno)
);