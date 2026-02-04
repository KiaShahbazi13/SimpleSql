#  -----------------------
# |                       |
# |   By @KiaShahbazi13   |
# |                       |
#  -----------------------
__version__ = "3.2.0"

class Sql():
    def __init__(self, db_path:str) -> None:
        import sqlite3
        self.db_path = db_path
        self.con = sqlite3.connect(db_path)
        self.cur = self.con.cursor()
        self.data = {}

    def sql_table(self, table_name:str, columns:str) -> None:
        '''
        table_name = the name of the table
        columns = the columns(NAME TYPE,...) e.g: 'id iteger PRIMARY KEY,name text,...'
        '''
        self.data[table_name] = {}
        self.data[table_name]["cols"] = []
        self.data[table_name]["rows"] = []
        for item in columns.split(','):
            self.data[table_name]["cols"].append(item.split(' ')[0])
        self.cur.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})")
        self.con.commit()

    def sql_insert(self, table_name:str, columns:str, v_num:str, values:tuple) -> None:
        '''
        table_name = the name of the table
        columns = the columns(NAME,...) e.g: 'id,name,...'
        v_num = number of values e.g: '?,?,?,...'
        values = the values(1,...) e.g: (123,32,)
        '''
        _ = []
        for item in values:
            _.append(item)
        self.data[table_name]["rows"].append(_)
        self.cur.execute(f"INSERT INTO {table_name} ({columns}) VALUES ({v_num})",values)
        self.con.commit()
    
    def sql_show(self, table_name:str, all=True, **kwargs):
        '''
        table_name = the name of the table \n
        all = show all of contents (True OR False) \n
        -if False : \n
        --kwargs : \n
        ---column = the column to show <column_name> OR "all" \n
        ---condition_count  int = the count of conditions \n
        ---condition_columns [] = the name of column the condition is on it \n
        ---condition_values  [] = the value for condition \n
        ---condition_v_types [] = the type of value for condition e.g: [int, float, str] \n
        ---condition_oprs    [] = the comparison-oprators for condition e.g: ["=",">"] \n
        '''
        return_value = []
        if all == True:
            exe = f'SELECT * FROM {table_name}'
            return_value.append(self.data[table_name]["cols"])
        else:
            try :
                cond_count = kwargs["condition_count"]
                cond_columns = kwargs["condition_columns"]
                cond_values = kwargs["condition_values"]
                cond_types = kwargs["condition_v_types"]
                cond_oprs = kwargs["condition_oprs"]
            except KeyError:
                return "Error:  ! Missing Kwargs !"
            if "column" in kwargs.keys():
                if kwargs["column"] == "all":
                    column = "*"
                    return_value.append(self.data[table_name]["cols"])
                else:
                    column = kwargs[column]
                    return_value.append(column)

            exe = f'SELECT {column} FROM {table_name}'


            if cond_types[0] == str:
                exe += f' Where {cond_columns[0]}{cond_oprs[0]}"{cond_values[0]}"'
            else:
                exe += f' Where {cond_columns[0]}{cond_oprs[0]}{cond_values[0]}'
            
            if cond_count > 1:
                for cond in range(kwargs["condition_count"]):
                    if cond_types[cond] == str:
                        exe += f' AND {cond_columns[cond]}{cond_oprs[cond]}"{cond_values[cond]}"'
                    else: 
                        exe += f' AND {cond_columns[cond]}{cond_oprs[cond]}{cond_values[cond]}'

            
        self.cur.execute(exe)
        rows = self.cur.fetchall()
        for row in rows:
                return_value.append(row)
        return return_value

    def sql_update(self, table_name:str, column:str, new_value, condition_column:str, condition_value, condition_opr) -> None:
        '''
        table_name = the name of the table
        column = the name of the column we want to change a item of
        new_value = the new value for the changed item
        condition_column = the name of column the condition is on it
        condition_value = the value for condition
        condition_opr = the comparison-oprators for condition ("=",">",...)
        '''
        self.cur.execute(f'UPDATE {table_name} set {column}="{new_value}" WHERE {condition_column}{condition_opr}{condition_value}')
        self.con.commit()

    def _check(self, l, table_name, cond_columns, cond_values, cond_oprs, cond):
        index = self.data[table_name]["cols"].index(cond_columns[cond])
        for row in self.data[table_name]["rows"]:
            if not row[0] in l:
                l[row[0]] = 0
            if cond_oprs[cond] == "=":
                if row[index] == cond_values[cond]:
                    l[row[0]] += 1
            elif cond_oprs[cond] == "!=":
                if row[index] != cond_values[cond]:
                    l[row[0]] += 1
            elif cond_oprs[cond] == ">":
                if row[index] > cond_values[cond]:
                    l[row[0]] += 1
            elif cond_oprs[cond] == "<":
                if row[index] < cond_values[cond]:
                    l[row[0]] += 1
        return l

    def sql_delete_row(self, table_name:str, condition=False, **kwargs):
        '''
        table_name = the name of the table
        condition = has condition(True, False)
        -if True :
        --kwargs :
        ---condition_count  int = the count of conditions
        ---condition_columns [] = the name of column the condition is on it
        ---condition_values  [] = the value for condition
        ---condition_v_types [] = the type of value for condition e.g: [int, float, str]
        ---condition_oprs    [] = the comparison-oprators for condition e.g: ["=",">"]
        '''
        exe = f'DELETE FROM {table_name}'
        if condition == True:
            l = {}
            if ["condition_columns" in kwargs.keys(), "condition_values" in kwargs.keys(), "condition_oprs" in kwargs.keys()] == [True, True, True] :
                cond_count = kwargs["condition_count"]
                cond_columns = kwargs["condition_columns"]
                cond_values = kwargs["condition_values"]
                cond_types = kwargs["condition_v_types"]
                cond_oprs = kwargs["condition_oprs"]
                if cond_types[0] == str:
                    exe += f' Where {cond_columns[0]}{cond_oprs[0]}"{cond_values[0]}"'
                else:
                    exe += f' Where {cond_columns[0]}{cond_oprs[0]}{cond_values[0]}'

                if cond_count > 1:
                    for cond in range(kwargs["condition_count"]):
                        if cond_types[cond] == str:
                            exe += f' AND {cond_columns[cond]}{cond_oprs[cond]}"{cond_values[cond]}"'
                        else: 
                            exe += f' AND {cond_columns[cond]}{cond_oprs[cond]}{cond_values[cond]}'
                        self._check(l, table_name, cond_columns, cond_values, cond_oprs, cond)
                else:
                    self._check(l, table_name, cond_columns, cond_values, cond_oprs, 0)
                
                _ = []
                for row in self.data[table_name]["rows"]:
                    if l[row[0]] == cond_count:
                        _.append(row)
                for item in _:
                    self.data[table_name]["rows"].remove(item)
        else:
            self.data[table_name].pop("rows")
        print(f"exe = -- {exe} --")
        self.cur.execute(exe)
        self.con.commit()

    def sql_delete_table(self, table_name:str):
        """
        table_name = the name of the table
        """
        self.data.pop(table_name)
        self.cur.execute(f'DROP TABLE IF EXISTS {table_name}')
        self.con.commit()
    
    def sql_delete_database(self):
        import os
        os.remove(self.db_name)
        del self