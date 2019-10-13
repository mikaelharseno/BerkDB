package db;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Database {
    // Various common constructs, simplifies parsing.
    private static final String REST = "\\s*(.*)\\s*",
            COMMA = "\\s*,\\s*",
            AND = "\\s+and\\s+";

    // Stage 1 syntax, contains the command name.
    private static final Pattern CREATE_CMD = Pattern.compile("create table " + REST),
            LOAD_CMD = Pattern.compile("load " + REST),
            STORE_CMD = Pattern.compile("store " + REST),
            DROP_CMD = Pattern.compile("drop table " + REST),
            INSERT_CMD = Pattern.compile("insert into " + REST),
            PRINT_CMD = Pattern.compile("print " + REST),
            SELECT_CMD = Pattern.compile("select " + REST);

    // Stage 2 syntax, contains the clauses of commands.
    private static final Pattern CREATE_NEW = Pattern.compile("(\\S+)\\s+\\((\\S+\\s+\\S+\\s*"
            + "(?:,\\s*\\S+\\s+\\S+\\s*)*)\\)"),
            SELECT_CLS = Pattern.compile("([^,]+?(?:,[^,]+?)*)\\s+from\\s+"
                    + "(\\S+\\s*(?:,\\s*\\S+\\s*)*)(?:\\s+where\\s+"
                    + "([\\w\\s+\\-*/'<>=!]+?(?:\\s+and\\s+"
                    + "[\\w\\s+\\-*/'<>=!]+?)*))?"),
            CREATE_SEL = Pattern.compile("(\\S+)\\s+as select\\s+"
                    + SELECT_CLS.pattern()),
            INSERT_CLS = Pattern.compile("(\\S+)\\s+values\\s+(.+?"
                    + "\\s*(?:,\\s*.+?\\s*)*)");
    private static final String EXIT = "exit";
    private static final String PROMPT = "> ";
    private HashMap<String, Table> wholeData;

    public Database() {
        wholeData = new HashMap<>();
    }

    private static String[] sameColumn(String[] cols1, String[] cols2) {
        //Assumes that variable types are the same and
        // no table have the same name type combination in
        String[] result = new String[cols1.length];
        int index = 0;
        for (int i = 0; i < cols1.length; i = i + 1) {
            for (int j = 0; j < cols2.length; j = j + 1) {
                if (cols1[i].equals(cols2[j])) {
                    result[index] = cols1[i];
                    index = index + 1;
                }
            }
        }
        String[] result1 = new String[index];
        for (int i = 0; i < index; i = i + 1) {
            result1[i] = result[i];
        }
        return result1;
    }

    private static Table join(Table A, Table B) {
        Table newtable;
        String[] overlap = sameColumn(A.colNames(), B.colNames());
        if (overlap == null) {
            newtable = A.cartesianCombine(B);
            return newtable;
        } else {
            newtable = A.selectiveCombine(B, overlap);
            return newtable;
        }
    }

    public static void main(String[] args) {
        Scanner in = new Scanner(System.in);
        Database db = new Database();
        System.out.print(PROMPT);

        String line = "";
        while ((line = in.nextLine()) != null) {
            if (EXIT.equals(line)) {
                break;
            }

            if (!line.trim().isEmpty()) {
                String result = db.transact(line);
                if (result.length() > 0) {
                    System.out.println(result);
                }
            }
            System.out.print(PROMPT);
        }

        in.close();
    }

    public boolean contains(String[] array, String val) {
        boolean result = false;
        for (String i : array) {
            if (i.compareTo(val) == 0) {
                result = true;
                break;
            }
        }
        return result;
    }

    private void createNewTable(String name, String col) {
        String[] cols = col.split(",");
        int numCol = cols.length;
        String[] split, names = new String[numCol], types = new String[numCol];
        String[] dataTypes = {"string", "int", "float"};
        for (int i = 0; i < numCol; i = i + 1) {
            split = cols[i].trim().split(" ");
            names[i] = split[0];
            types[i] = split[split.length - 1];
            if (!contains(dataTypes, types[i])) {
                throw new RuntimeException("invalid data type");
            }
        }
        wholeData.put(name.trim(), new Table(names, types));
    }

    private void createSelectedTable(String name, String exprs, String tables, String conds) {
        Table target = select(exprs, tables, conds);
        wholeData.put(name, target);
    }

    private String loadTable(String name) {
        try {
            FileReader f = new FileReader(name + ".tbl");
            BufferedReader input = new BufferedReader(f);

            String header = input.readLine();
            if (header == null) {
                throw new RuntimeException("missing header in .tbl file");
            }
            String[] columns = header.split(",");
            String[][] temp = new String[columns.length][2];
            for (int i = 0; i < columns.length; i++) {
                temp[i] = columns[i].split("\\s+");
            }

            int columnNamesctr = 0, columnTypesctr = 0;
            String[] columnNames = new String[columns.length];
            for (int i = 0; i < columns.length; i++, columnNamesctr++) {
                columnNames[i] = temp[i][0];
            }

            String[] columnTypes = new String[columns.length];
            for (int i = 0; i < columns.length; i++, columnTypesctr++) {
                columnTypes[i] = temp[i][1];
            }

            if (columnNamesctr != columnTypesctr) {
                throw new RuntimeException(
                        "Unmatching number of names and types in TBL file");
            }

            Table table = new Table(columnNames, columnTypes);

            String curr;
            while ((curr = input.readLine()) != null) {
                String[] data = curr.split(",");

                if (data.length != columnNames.length) {
                    throw new RuntimeException(
                            "Unmathching row count and column count in TBL file");
                }

                table.putRow(data);
            }

            wholeData.put(name, table);
            f.close();
            return "";

        } catch (FileNotFoundException e) {
            return "ERROR: " + e.getMessage();
        } catch (IOException e) {
            return "ERROR: " + e.getMessage();
        }

//        System.out.printf("You are trying to load the table named %s\n", name);

    }

    private String storeTable(String name) {
        try {
            if (wholeData.containsKey(name)) {
                FileWriter file = new FileWriter(name + ".tbl");
                BufferedWriter bw = new BufferedWriter(file);
                bw.write(wholeData.get(name).printTableString());
                bw.close();
                return "";
            } else {
                throw new RuntimeException("No such table: " + name);
            }

        } catch (IOException e) {
            return "ERROR: " + e.getMessage();
        }
//        System.out.printf("You are trying to store the table named %s\n", name);
    }

    private String dropTable(String name) {
//        try {
        if (wholeData.containsKey(name)) {
            wholeData.remove(name);
            return "";
        } else {
            throw new RuntimeException("no such file: " + name);
        }
//        } catch (Exception e) {
//            return "ERROR: " + e.getMessage();
//        }
    }

    private String insertRow(String expr) {
//        try {
        Matcher m = INSERT_CLS.matcher(expr);
        if (!m.matches()) {
            throw new RuntimeException("Malformed insert: " + expr);
        }
        //Checks if table is in database
        if (wholeData.containsKey(m.group(1))) {
            if (wholeData.get(m.group(1)).colNum()
                    == m.group(2).split(COMMA).length) {
                wholeData.get(m.group(1)).putRow(m.group(2).split(COMMA));
                return "";
            } else {
                //Error statement. Input row length and number of columns not the same.
                throw new RuntimeException("Row length and column length does not match");
            }

        } else { //Error statement. The target table does not exist in database.
            throw new RuntimeException("No such table: " + m.group(1));
        }
//        } catch (Exception e) {
//            return "ERROR: " + e.getMessage();
//        }
    }

    private String printTable(String name) {
//        try {
        if (wholeData.containsKey(name)) {
            return wholeData.get(name).printTableString();
        } else {
            throw new RuntimeException("No such table: " + name);
        }
//        } catch (Exception e) {
//            return "ERROR: " + e.getMessage();
//        }
    }

    private Table select(String expr) {

        Matcher m = SELECT_CLS.matcher(expr);
        if (!m.matches()) {
            throw new RuntimeException("Malformed select: " + expr);
        }

        return select(m.group(1), m.group(2), m.group(3));

    }

    private Table select(String exprs, String tables, String conds) {
//        System.out.printf("You are trying to select these expressions:" +
//                " '%s' from the join of these tables: '%s',
//                filtered by these conditions: '%s'\n", exprs, tables, conds);
//        try {
        if (tables.equals("")) {
            throw new RuntimeException("No such table found");
        }
        tables = Table.whiteSpaceKiller(tables);
        String[] tableNames = tables.split(",");

        //Joint table generation
        //Error detecting for loop (PLACE HERE) - detects invalid table names
        for (String w : tableNames) {
            if (!wholeData.containsKey(w)) {
                throw new RuntimeException("No such table found");
            }
        }
        Table central = wholeData.get(tableNames[0]);
        if (tableNames.length > 1) {
            for (int i = 1; i < tableNames.length; i = i + 1) {
                central = Database.join(central, wholeData.get(tableNames[i]));
            }
        }

        //SelectByColumn
        //Error detecting for loop (PLACE HERE) - detects invalid expressions
        if (!exprs.equals("*")) {
            String[] expNames = exprs.split(",");
            central = central.selectByColumns(expNames);
        }

        //SelectByRow (Cond must be checked in method)
        if (conds != null) {
            String[] conditions = conds.split(" and ");
            central = central.selectByRows(conditions);
        }
        return central;
    }


    public String transact(String query) {
        // Mulai dari yang gampang dulu
        try {
            Matcher m, n;
            String result = "";
            if ((m = CREATE_CMD.matcher(query)).matches()) {
                if ((n = CREATE_NEW.matcher(m.group(1))).matches()) {
                    createNewTable(n.group(1), n.group(2)); //Done
                } else if ((n = CREATE_SEL.matcher(m.group(1))).matches()) {
                    createSelectedTable(n.group(1), n.group(2),
                            n.group(3), n.group(4)); //Framework done.
                } else {
                    throw new RuntimeException("Malformed create ");
                }
            } else if ((m = LOAD_CMD.matcher(query)).matches()) {
                result = loadTable(m.group(1)); //Done
            } else if ((m = STORE_CMD.matcher(query)).matches()) {
                result = storeTable(m.group(1)); //Done
            } else if ((m = DROP_CMD.matcher(query)).matches()) {
                result = dropTable(m.group(1)); //Done
            } else if ((m = INSERT_CMD.matcher(query)).matches()) {
                result = insertRow(m.group(1)); //Done. Tinggal error2 nya.
            } else if ((m = PRINT_CMD.matcher(query)).matches()) {
                result = printTable(m.group(1)); //Done
            } else if ((m = SELECT_CMD.matcher(query)).matches()) {
                result = select(m.group(1)).printTableString(); //Framework done.
            } else {
                result = "ERROR: Malformed query: " + query;
            }

            return result;
        } catch (RuntimeException e) {
            return "ERROR: " + e.getMessage();
        }
    }
}
