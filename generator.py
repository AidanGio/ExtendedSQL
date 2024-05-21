import subprocess
import os

def getUserInput():
    if os.path.isfile("input.txt"):
        print("Reading input from file: input.txt")
        with open("input.txt", "r") as file:
            lines = file.readlines()
            S = lines[0][4:].strip()
            n = int(lines[1][4:].strip())
            V = lines[2][4:].strip()
            F = lines[3][4:].strip()
            sigma = lines[4][4:].split(',')
            G = lines[5][4:].strip()
    else:
        print("input.txt not found. Prompting user for input.")
        S = input("Enter the SELECT attributes (comma-separated): ")
        n = int(input("Enter the number of grouping variables: "))
        V = input("Enter the grouping attributes (comma-separated): ")
        F = input("Enter the aggregate functions (comma-separated): ")
        sigma = []
        for i in range(n):
            sigma.append(input(f"Enter the condition for grouping variable {i+1}: "))
        G = input("Enter the HAVING condition: ")

    S = [attr.strip() for attr in S.split(',')]
    V = [attr.strip() for attr in V.split(',')]
    F = [func.strip() for func in F.split(',')]
    G = G.strip()
    sigma = [condition.strip() for condition in sigma]
    phi = [S, n, V, F, sigma, G]
    return phi

def groupingAttr(V):
    key = ', '.join([f"row['{attr}']" for attr in V])
    key = f"({key},)"
    return key

def addToH(F):
    result = ""
    for item in F:
        # print(item)
        if item[1] == 'sum':
            # Creating a tuple from the item list
            result += f"{tuple(item)}: 0, "
        elif item[1] == 'count':
            result += f"{tuple(item)}: 0, "
        elif item[1] == 'avg':
            result += f"{tuple(item)}: [0, 0], "
        elif item[1] == 'min':
            result += f"{tuple(item)}: float('inf'), "
        elif item[1] == 'max':
            result += f"{tuple(item)}: float('-1'), "
    result = result.rstrip(', ')
    # print(result)
    return result

def formatSigma(sigma):
    # print(sigma)
    if sigma == [['']]:
        return ""
    result = ""
    for i in range(len(sigma)):
        if i == 0:
            looping = True
            operator = 4
            result += (f"if row['{sigma[i][1]}'] {sigma[i][2]} {sigma[i][3]}")
            while looping:
                if operator >= len(sigma[i]):
                    break
                if sigma[i][operator] == 'and' or sigma[i][operator] == 'or':
                    result += (f" {sigma[i][operator]} row['{sigma[i][operator+2]}'] {sigma[i][operator+3]} {sigma[i][operator+4]}")
                    operator += 5
                else:
                    looping = False
            result += (":\n")
            result += (f"            grouping_var = {sigma[i][0]}\n")
        else:
            result += (f"        elif row['{sigma[i][1]}'] {sigma[i][2]} {sigma[i][3]}")
            looping = True
            operator = 4
            while looping == True:
                if operator >= len(sigma[i]):
                    break
                if sigma[i][operator] == 'and' or sigma[i][operator] == 'or':
                    result += (f" {sigma[i][operator]} row['{sigma[i][operator+2]}'] {sigma[i][operator+3]} {sigma[i][operator+4]}")
                    operator += 5
                else:
                    looping = False
            result += (":\n")
            result += (f"            grouping_var = {sigma[i][0]}\n")
    # print(result)
    return result
        
def havingClause(G):
    result = ''
    if G == '':
        return True
    else:
        G = G.split(' ')
        for i in range(len(G)):
            G[i] = G[i].split('_')
        # print(G)
        for i in range(len(G)):
                # print(G[i])
                if G[i][0] == 'and' or G[i][0] == 'or':
                    result += f" {G[i][0]} "
                elif len(G[i]) == 3:
                    result += f"result[('{G[i][0]}', '{G[i][1]}', '{G[i][2]}')]"
                else:
                    result += f" {G[i][0]} "
    
        # print(result)
        return result

def generateMfQuery(phi):
    S, n, V, F, sigma, G = phi
    for i in range(len(F)):
        F[i] = F[i].split('_')
    for i in range(len(sigma)):
        sigma[i] = sigma[i].split(' ')
    
    grouping_keys = ''
    for i in range(len(V)):
        grouping_keys += f"'{V[i]}': key[{i}], "
    grouping_keys = grouping_keys.rstrip(', ')

    body = f"""
    H = {{}}
    for row in data:
        key = {groupingAttr(V)}
        if key not in H:
            H[key] = {{{addToH(F)}}}

    for row in data:
        key = {groupingAttr(V)}
        grouping_var = 0
        F = {F}
        for func in F:
            if int(func[0]) == grouping_var:
                field = func[2]
                func_type = func[1]
                # print("Updating:", key, func, "with", row[field])  # Debugging
                if func_type == 'sum':
                    H[key][(func[0], func_type, field)] += row[field]
                elif func_type == 'count':
                    H[key][(func[0], func_type, field)] += 1
                elif func_type == 'min':
                    H[key][(func[0], func_type, field)] = min(H[key][(func[0], func_type, field)], row[field])
                elif func_type == 'max':
                    H[key][(func[0], func_type, field)] = max(H[key][(func[0], func_type, field)], row[field])
                elif func_type == 'avg':
                    H[key][(func[0], func_type, field)][0] += row[field]
                    H[key][(func[0], func_type, field)][1] += 1

    for row in data:
        key = {groupingAttr(V)}
        grouping_var = -1
        {formatSigma(sigma)}
        F = {F}
        for func in F:
            if int(func[0]) == grouping_var:
                field = func[2]
                func_type = func[1]
                # print("Updating:", key, func, "with", row[field])  # Debugging
                if func_type == 'sum':
                    H[key][(func[0], func_type, field)] += row[field]
                elif func_type == 'count':
                    H[key][(func[0], func_type, field)] += 1
                elif func_type == 'min':
                    H[key][(func[0], func_type, field)] = min(H[key][(func[0], func_type, field)], row[field])
                elif func_type == 'max':
                    H[key][(func[0], func_type, field)] = max(H[key][(func[0], func_type, field)], row[field])
                elif func_type == 'avg':
                    H[key][(func[0], func_type, field)][0] += row[field]
                    H[key][(func[0], func_type, field)][1] += 1

    _global = []
    # print(H.items())
    for key, value in H.items():
        result = {{{grouping_keys}}}
        for func_key, func_value in value.items(): # func_key = ('1', 'avg', 'quant'), func_value = [0, 0]
            if 'avg' in func_key:
                if func_value[1] == 0:
                    result[func_key] = 0
                else:
                    result[func_key] = func_value[0] / func_value[1]
            else:
                result[func_key] = func_value
        inf = float('inf')
        if {havingClause(G)}:
            _global.append(result)
    """
    return body

def generateCode(body):

    generateCode = f"""
import os
import psycopg2
import psycopg2.extras
import tabulate
from dotenv import load_dotenv

# DO NOT EDIT THIS FILE, IT IS GENERATED BY generator.py



def query():
    load_dotenv()

    user = os.getenv('USERNAME')
    password = os.getenv('PASSWORD')
    dbname = os.getenv('DBNAME')

    conn = psycopg2.connect("dbname="+dbname+" user="+user+" password="+password,
                            cursor_factory=psycopg2.extras.DictCursor)
    cur = conn.cursor()
    cur.execute("SELECT * FROM sales")
    data = cur.fetchall()

    {body}
    
    return tabulate.tabulate(_global,
                        headers="keys", tablefmt="psql")


def main():
    print(query())
    
if "__main__" == __name__:
    main()
    """
    return generateCode
    

def main():
    phi = getUserInput()
    # print(phi)
    generatedQuery = generateMfQuery(phi)
    # print(generatedQuery)
    generatedCode = generateCode(generatedQuery)
    open("_generated.py", "w").write(generatedCode)
    subprocess.run(["python3", "_generated.py"])


if "__main__" == __name__:
    main()