#  -----------------------
# |                       |
# |   By @KiaShahbazi13   |
# |                       |
#  -----------------------
__version__ = "3.2.3"



class Sql():
    def __init__(self, db_path:str, check_same_thread=True) -> None:
        import sqlite3
        self.db_path = db_path
        self.con = sqlite3.connect(self.db_path, check_same_thread=check_same_thread)
        self.cur = self.con.cursor()
        self.data = {}
        self.tables = []
        self._load()

    def __load_tb(self, table):
            """"""
            self.tables.append(table[0])
            self.data[table[0]] = {}
            self.data[table[0]]["cols"] = []
            exe = f"SELECT * FROM '{table[0]}'"
            _data = self.cur.execute(exe)
            for column in _data.description:
                self.data[table[0]]["cols"].append(column[0])
    
    def _load(self, _tables=False):
        """Finds all tables in a database"""
        exe = f'SELECT name from sqlite_master where type="table"'
        self.cur.execute(exe)
        tables = self.cur.fetchall()

        if _tables == True:
            return tables
        elif _tables == False:
            for table in tables:
                self.__load_tb(table)

    def sql_table(self, table_name:str, columns:str) -> None:
        '''
        table_name = the name of the table \n
        columns = the columns(NAME TYPE,...) e.g: 'id iteger PRIMARY KEY,name text,...'
        '''
        self.cur.execute(f"CREATE TABLE IF NOT EXISTS '{table_name}' ({columns})")
        self.con.commit()
        self.__load_tb((table_name,))

    def sql_insert(self, table_name:str, columns:str, v_num:str, values:tuple) -> None:
        '''
        table_name = the name of the table\n
        columns = the columns(NAME,...) e.g: 'id,name,...'\n
        v_num = number of values e.g: '?,?,?,...'\n
        values = the values(1,...) e.g: (123,32,)
        '''
        print()
        print(f"sql query:")
        print(f"INSERT INTO {table_name} ({columns}) VALUES ({v_num})",values)
        print()
        self.cur.execute(f"INSERT INTO {table_name} ({columns}) VALUES ({v_num})",values)
        self.con.commit()
    
    def sql_show(self, table_name:str, all=True, **kwargs):
        '''
        table_name = the name of the table \n
        all = show all of contents (True OR False) \n
        -if False : \n
        --kwargs : \n
        ---columns str/[] = the column(s) to show -> [<column_name>,] OR "all" \n
        ---condition_columns  [] = the name of column the condition is on it \n
        ---condition_values   [] = the value for condition \n
        ---condition_oprs     [] = the comparison-oprators for condition e.g: ["=",">"] \n
        ---condition_sep_oprs [] = the comparison-oprators for seperating conditions e.g: ["and","or"] \n
        '''
        if not table_name in self.tables:
            raise ValueError(f"No tables in database whith this name: '{table_name}'")
        return_value = []
        if all == True:
            exe = f"SELECT * FROM '{table_name}'"
            return_value.append(self.data[table_name]["cols"])
        else:
            try:
                exe = "SELECT"
                _cols = self.data[table_name]["cols"]
                if kwargs["columns"] == "all":
                    exe += f" *"
                    return_value.append(_cols)
                else:
                    return_value.append([])
                    for column in kwargs["columns"]:
                        exe += f" {column},"
                        return_value[0].append(_cols[_cols.index(column)])
                    exe = exe.strip(",")
                exe += f" FROM '{table_name}'"
            except KeyError:
                return "Error:  ! Missing Kwargs !"
        
        if "condition_columns" in kwargs.keys():
            cond_columns = kwargs["condition_columns"]
            cond_values = kwargs["condition_values"]
            cond_oprs = kwargs["condition_oprs"]
            if "condition_sep_oprs" in kwargs.keys(): cond_sep_oprs = kwargs["condition_sep_oprs"]
            else: cond_sep_oprs = []

            exe += f' WHERE '
            for cond in cond_columns:
                _index = cond_columns.index(cond)
                exe += f" {cond} {cond_oprs[_index]} '{cond_values[_index]}'"
                try:
                    exe += f" {cond_sep_oprs[_index]}"
                except IndexError:
                    pass
        
        self.cur.execute(exe)
        rows = self.cur.fetchall()
        for row in rows:
                return_value.append(row)
        if len(return_value) < 2:
            return None
        return return_value

    def sql_update(self, table_name:str, column:str, new_value, all=True, **kwargs) -> None:
        '''
        table_name = the name of the table\n
        column = the name of the column we want to change a item of\n
        new_value = the new value for the changed item\n
        all = update all records (True OR False) \n
        -if False : \n
        --kwargs : \n
        ---condition_columns  [] = the name of column the condition is on it \n
        ---condition_values   [] = the value for condition \n
        ---condition_oprs     [] = the comparison-oprators for condition e.g: ["=",">"] \n
        ---condition_sep_oprs [] = the comparison-oprators for seperating conditions e.g: ["and","or"] \n
        '''

        if all == True:
            exe = f"UPDATE '{table_name}' set {column}='{new_value}'"
        else:
            exe = f"UPDATE '{table_name}' set"
        
        if "condition_columns" in kwargs.keys():
            cond_columns = kwargs["condition_columns"]
            cond_values = kwargs["condition_values"]
            cond_oprs = kwargs["condition_oprs"]
            if "condition_sep_oprs" in kwargs.keys(): cond_sep_oprs = kwargs["condition_sep_oprs"]
            else: cond_sep_oprs = []

            exe += f' WHERE '
            for cond in cond_columns:
                _index = cond_columns.index(cond)
                exe += f" {cond} {cond_oprs[_index]} '{cond_values[_index]}'"
                try:
                    exe += f" {cond_sep_oprs[_index]}"
                except IndexError:
                    pass

        self.cur.execute(exe)
        self.con.commit()

    def sql_delete_row(self, table_name:str, condition=False, **kwargs):
        '''
        table_name = the name of the table\n
        condition = has condition(True, False)\n
        -if True :\n
        --kwargs :\n
        ---condition_columns  [] = the name of column the condition is on it \n
        ---condition_values   [] = the value for condition \n
        ---condition_oprs     [] = the comparison-oprators for condition e.g: ["=",">"] \n
        ---condition_sep_oprs [] = the comparison-oprators for seperating conditions e.g: ["and","or"] \n
        '''
        exe = f'DELETE FROM {table_name}'
        if condition == True:
            if "condition_columns" in kwargs.keys():
                cond_columns = kwargs["condition_columns"]
                cond_values = kwargs["condition_values"]
                cond_oprs = kwargs["condition_oprs"]
                if "condition_sep_oprs" in kwargs.keys(): cond_sep_oprs = kwargs["condition_sep_oprs"]
                else: cond_sep_oprs = []

                exe += f' WHERE '
                l = {}
                for cond in cond_columns:
                    _index = cond_columns.index(cond)
                    exe += f" {cond} {cond_oprs[_index]} '{cond_values[_index]}'"
                    try:
                        exe += f" {cond_sep_oprs[_index]}"
                    except IndexError:
                        pass
            
        else:
            self.data[table_name].pop("rows")
        self.cur.execute(exe)
        self.con.commit()

    def sql_delete_table(self, table_name:str):
        """
        table_name = the name of the table\n
        """
        try:
            self.data.pop(table_name)
        except:
            pass
        self.cur.execute(f'DROP TABLE IF EXISTS {table_name}')
        self.con.commit()
    
    def sql_delete_database(self):
        import os
        os.remove(self.db_path)
        del self

    def sql_close_connection(self):
        self.con.close()
        del self

if __name__ == "__main__":
    pass