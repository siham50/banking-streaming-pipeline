CREATE OR REFRESH STREAMING TABLE OBT_silver
AS
   SELECT 
        st.transaction_id, st.transaction_reference, st.customer_id, st.payee_id, st.account_id, st.source_location_id, st.destination_location_id, st.channel_id, st.card_network_id, st.payment_method_id, st.transaction_status_id, st.branch_id, st.beneficiary_branch_id, st.decline_reason_id, st.customer_name, st.customer_email, st.customer_phone, st.payee_name, st.payee_risk_score,st.payee_phone, st.payee_tax_id, st.account_type, st.card_tier, st.account_number, st.customer_address, st.customer_latitude, st.customer_longitude, st.payee_address, st.payee_latitude, st.payee_longitude, st.transaction_amount, st.processing_duration, st.initiation_timestamp, st.transaction_timestamp, st.settlement_timestamp, st.base_fee, st.variable_fee,st.processing_fee, st.risk_multiplier, st.subtotal,st.total_amount, st.feedback_score, cn.card_network, b.branch AS customer_branch, b.city AS customer_city, b.region AS customer_region, b.updated_at AS customer_updated_at, b2.branch AS beneficiary_branch, b2.city AS beneficiary_city, b2.region AS beneficiary_region, b2.updated_at AS beneficiary_updated_at, c.channel, c.description, c.base_rate, c.percentage_fee, c.per_minute, dr.decline_reason, pm.payment_method, pm.is_card, pm.requires_auth, ts.transaction_status, ts.is_completed    
    FROM STREAM (banking.bronze.staging_transactions)
    WATERMARK transaction_timestamp DELAY OF INTERVAL 4 MINUTES
    as st
    LEFT JOIN banking.bronze.map_card_networks as cn
    ON st.card_network_id = cn.card_network_id
    LEFT JOIN banking.bronze.map_branches as b
    ON st.branch_id = b.branch_id
    LEFT JOIN banking.bronze.map_branches as b2
    ON st.beneficiary_branch_id = b2.branch_id
    LEFT JOIN banking.bronze.map_channels as c
    ON st.channel_id = c.channel_id
    LEFT JOIN banking.bronze.map_decline_reasons as dr
    ON st.decline_reason_id = dr.decline_reason_id
    LEFT JOIN banking.bronze.map_payment_methods as pm
    ON st.payment_method_id = pm.payment_method_id
    LEFT JOIN banking.bronze.map_transaction_statuses as ts
    ON st.transaction_status_id = ts.transaction_status_id
          

          