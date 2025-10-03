def import_from_xlsx(filename):
    import pandas as pd
    xlsx_df = pd.read_excel(filename)
    return xlsx_df

a = import_from_xlsx("users.xlsx")
print(a)