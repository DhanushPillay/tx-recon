import great_expectations as gx
import great_expectations.expectations as gxe
import os
import glob
import sys

def validate_latest_settlement():
    # Find latest csv in data/
    files = glob.glob("data/settlement_*.csv")
    if not files:
        print("No settlement file found.")
        sys.exit(1)
        
    latest_file = max(files, key=os.path.getctime)
    print(f"Validating {latest_file} with Great Expectations...")
    
    # Load into Great Expectations 1.x Context
    context = gx.get_context(mode="ephemeral")
    data_source = context.data_sources.add_pandas("pandas")
    data_asset = data_source.add_csv_asset("settlement_csv", filepath_or_buffer=latest_file)
    batch_def = data_asset.add_batch_definition_whole_dataframe("whole_df")
    
    # Define the Data Contract
    suite = context.suites.add(gx.ExpectationSuite(name="settlement_suite"))
    
    # 1. Transaction ID must be unique (no duplicate settlements)
    suite.add_expectation(gxe.ExpectColumnValuesToBeUnique(column="transaction_id"))
    # 2. Settled amount must be strictly positive
    suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="settled_amount_paise", min_value=1, max_value=9999999999))
    # 3. Bank Ref ID must not be null
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="bank_ref_id"))
    
    validation_definition = context.validation_definitions.add(
        gx.ValidationDefinition(
            name="settlement_validation",
            data=batch_def,
            suite=suite,
        )
    )
    
    results = validation_definition.run()
    
    if results.success:
        print("SUCCESS: Data Contract Validated successfully!")
        sys.exit(0)
    else:
        print("ERROR: Data Contract Validation FAILED!")
        sys.exit(1)

if __name__ == "__main__":
    validate_latest_settlement()
