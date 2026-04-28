
"""
This module creates Aggregation / Data Mart tables for BI Reporting (Looker Studio).
All tables are aggregated at the Application Level (sk_id_curr).
"""


def agg_bureau_summary_spark():
    """
    Behavioral History at External Credit Bureau.
    Summary of customer's credit history at external institutions.
    """
    print("Aggregating Bureau Summary...")
    df_bureau = read_bq("bureau") # Read from Silver
    
    # Create auxiliary flags
    df_bureau = df_bureau.withColumn("is_active", when(col("credit_active") == "Active", 1).otherwise(0))
    df_bureau = df_bureau.withColumn("is_bad_debt", when(col("credit_active") == "Bad debt", 1).otherwise(0))
    df_bureau = df_bureau.withColumn("is_overdue", when(col("amt_credit_sum_overdue") > 0, 1).otherwise(0))

    agg_df = df_bureau.groupBy("sk_id_curr").agg(
        count("sk_id_bureau").alias("bureau_total_loans"),
        sum("is_active").alias("bureau_active_loans"),
        sum("is_bad_debt").alias("bureau_bad_debts"),
        sum("is_overdue").alias("bureau_loans_with_overdue"),
        
        sum("amt_credit_sum").alias("bureau_total_credit_amt"),
        sum("amt_credit_sum_debt").alias("bureau_total_debt_amt"),
        sum("amt_credit_sum_overdue").alias("bureau_total_overdue_amt"),
        
        # Average Debt-to-Credit Ratio
        avg(when(col("amt_credit_sum") > 0, col("amt_credit_sum_debt") / col("amt_credit_sum")).otherwise(0)).alias("bureau_avg_debt_ratio")
    )
    
    # Fill null values with 0 after aggregation
    agg_df = agg_df.fillna(0)
    write_bq(agg_df, "mart_bureau_summary")
    return agg_df

def agg_previous_app_summary_spark():
    """
    Behavioral History summary at Home Credit.
    Summary of application history within the organization.
    """
    print("Aggregating Previous Application Summary...")
    df_prev = read_bq("previous_application")
    
    # Create flags
    df_prev = df_prev.withColumn("is_approved", when(col("name_contract_status") == "Approved", 1).otherwise(0))
    df_prev = df_prev.withColumn("is_refused", when(col("name_contract_status") == "Refused", 1).otherwise(0))
    
    agg_df = df_prev.groupBy("sk_id_curr").agg(
        count("sk_id_prev").alias("prev_total_apps"),
        sum("is_approved").alias("prev_approved_apps"),
        sum("is_refused").alias("prev_refused_apps"),
        
        sum("amt_application").alias("prev_total_applied_amt"),
        sum("amt_credit").alias("prev_total_granted_credit"),
        
        # Average down payment amount
        avg("amt_down_payment").alias("prev_avg_down_payment")
    )
    
    # Calculate Refusal Rate - extremely important feature for Risk
    agg_df = agg_df.withColumn(
        "prev_refusal_rate", 
        round(col("prev_refused_apps") / col("prev_total_apps"), 4)
    )
    
    agg_df = agg_df.fillna(0)
    write_bq(agg_df, "mart_previous_app_summary")
    return agg_df

def agg_payment_behavior_spark():
    """
    Additional function: Summary of installment payment behavior.
    Very important for visualization in Looker Studio to analyze late payments.
    """
    print("Aggregating Payment Behavior Summary...")
    df_ins = read_bq("installments_payments")
    
    # Calculate days late (actual payment date minus scheduled payment date)
    # Negative = early payment, Positive = late payment
    df_ins = df_ins.withColumn("days_late", col("days_entry_payment") - col("days_instalment"))
    df_ins = df_ins.withColumn("is_late", when(col("days_late") > 0, 1).otherwise(0))
    df_ins = df_ins.withColumn("is_underpaid", when(col("amt_payment") < col("amt_instalment"), 1).otherwise(0))

    agg_df = df_ins.groupBy("sk_id_curr").agg(
        count("sk_id_prev").alias("pay_total_installments"),
        sum("is_late").alias("pay_late_installments"),
        sum("is_underpaid").alias("pay_underpaid_installments"),
        
        max("days_late").alias("pay_max_days_late"),
        avg("days_late").alias("pay_avg_days_late"),
        
        sum("amt_instalment").alias("pay_total_required_amt"),
        sum("amt_payment").alias("pay_total_paid_amt")
    )
    
    # Late payment rate
    agg_df = agg_df.withColumn(
        "pay_late_rate", 
        round(col("pay_late_installments") / col("pay_total_installments"), 4)
    )
    
    agg_df = agg_df.fillna(0)
    write_bq(agg_df, "mart_payment_behavior")
    return agg_df

def create_master_looker_datamart_spark():
    """
    Additional function: Create a wide Master Table (Flat Table) for Looker Studio.
    This table joins Application with all summary tables above.
    """
    print("Creating Master Data Mart for Looker Studio...")
    
    # Get current application data & target (for risk analysis)
    df_app = read_bq("application") # Select necessary columns or call Fact_Application builder
    df_target = read_bq("application_target")
    
    # Get summary tables
    df_bureau = read_bq("mart_bureau_summary")
    df_prev = read_bq("mart_previous_app_summary")
    df_pay = read_bq("mart_payment_behavior")
    
    # Join all tables using sk_id_curr (Left Join to retain new customers)
    master_df = df_app.join(df_target, on="sk_id_curr", how="inner") \
                      .join(df_bureau, on="sk_id_curr", how="left") \
                      .join(df_prev, on="sk_id_curr", how="left") \
                      .join(df_pay, on="sk_id_curr", how="left")
                      
    # Fill default values for customers with no history (new customers)
    master_df = master_df.fillna(0, subset=[c for c in master_df.columns if c.startswith(("bureau_", "prev_", "pay_"))])
    
    # Categorize Risk for Looker Studio (easy for Pie Chart / Bar Chart)
    master_df = master_df.withColumn(
        "risk_label", 
        when(col("target") == 1, "Default (Risk)").otherwise("Good (No Risk)")
    )
    
    write_bq(master_df, "looker_master_datamart")
    print("Master Data Mart created successfully!")
    return master_df