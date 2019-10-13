package db;

import java.util.HashMap;

/**
 * Created on 2/24/2017.
 */
public class Table {
    private String[] colNames;
    private String[] colTypes;
    private HashMap<Integer, String[]> data;
    private int counter;
    private int colNum;

    public Table(String[] names, String[] types) { // Constructor with arguments
        data = new HashMap<>();
        colNames = names;
        colTypes = types;
        colNum = names.length;
        counter = 0;
    }

    public static float aritmaticOp(String op, float a, float b) {
        if (op.equals("+")) {
            return a + b;
        } else if (op.equals("-")) {
            return a - b;
        } else if (op.equals("*")) {
            return a * b;
        } else if (op.equals("/")) {
            return a / b;
        } else {
            throw new RuntimeException();
        }
    }

    private static String floatThreePointer(String s) {
        if (s.equals("NaN")) {
            return s;
        }
        if (s.equals("NOVALUE")) {
            return s;
        }
        String[] asd = s.split("\\.");
        if (asd.length == 1) {
            return s + ".000";
        }
        if (asd[1].length() == 0) {
            return s + "000";
        } else if (asd[1].length() == 1) {
            return s + "00";
        } else if (asd[1].length() == 2) {
            return s + "0";
        } else {
            return asd[0] + "." + asd[1].substring(0, 3);
        }
    }

    public static int aritmaticOpI(String op, int a, int b) {
        if (op.equals("+")) {
            return a + b;
        } else if (op.equals("-")) {
            return a - b;
        } else if (op.equals("*")) {
            return a * b;
        } else {
            throw new RuntimeException();
        }
    }

    private static String[] reducer(String[] cols, String[] takenOut) {
        if (takenOut == null) {
            return cols;
        }
        String[] result = new String[cols.length - takenOut.length];
        int index = 0;
        boolean same = false;
        for (int i = 0; i < cols.length; i = i + 1) {
            same = false;
            for (int j = 0; j < takenOut.length; j = j + 1) {
                if (cols[i].equals(takenOut[j])) {
                    same = true;
                }
            }
            if (same == false) {
                result[index] = cols[i];
                index = index + 1;
            }
        }
        return result;
    }

    public static String dataType(String input) {
        try {
            if (input.equals("NOVALUE")) {
                return "novalue";
            }
            if (input.equals("NaN")) {
                return "nan";
            }
            int length = input.length();
            String[] inputsplit = input.split("");
            if (inputsplit[0].equals("'") && inputsplit[length - 1].equals("'")) {
                return "string";
            } else {
                if (input.equals(".")) {
                    throw new RuntimeException("Input not valid."); //Bisa diubah nanti
                }
                int dotcounter = 0;
                for (String w : inputsplit) {
                    if (w.equals(".")) {
                        dotcounter = dotcounter + 1;
                    }
                    if (!(w.equals("1") || w.equals("2") || w.equals("3") || w.equals("4") || w.equals("5") || w.equals("6")
                            || w.equals("7") || w.equals("8") || w.equals("9") || w.equals(".") || w.equals("0"))) {
                        throw new RuntimeException("Input not valid."); //Bisa diubah nanti
                    }
                }
                if (dotcounter == 0) {
                    return "int";
                } else if (dotcounter == 1) {
                    return "float";
                } else {
                    throw new RuntimeException("Input not valid."); //Bisa diubah nanti
                }
            }
        } catch (Exception e) {
            return "ERROR: " + e.getMessage();
        }
    }

    public static String whiteSpaceKiller(String input) {
        String[] abc = input.split(" ");
        String output = "";
        for (String u : abc) {
            output = output + u;
        }
        return output;
    }

    public String[] colNames() {
        return colNames;
    }

    public String[] colTypes() {
        return colTypes;
    }

    public int size() {
        return counter;
    }

    public int colNum() {
        return colNum;
    }

    public String[] getRow(int index) {
        return data.get(index);
    }

    public Table cartesianCombine(Table target) {
        String[] newColNames = new String[colNames.length + target.colNames().length];
        System.arraycopy(colNames, 0, newColNames, 0, colNames.length);
        System.arraycopy(target.colNames(), 0, newColNames, colNames.length, target.colNames().length);
        String[] newColTypes = new String[colNames.length + target.colNames().length];
        System.arraycopy(colTypes, 0, newColTypes, 0, colNames.length);
        System.arraycopy(target.colTypes(), 0, newColTypes, colNames.length, target.colNames().length);
        Table result = new Table(newColNames, newColTypes);
        int newColNum = colNum() + target.colNum();
        for (int i = 0; i < counter; i = i + 1) {
            for (int j = 0; j < target.size(); j = j + 1) {
                String[] input = new String[newColNum];
                String[] targetRow = target.getRow(j);
                for (int k = 0; k < colNum; k = k + 1) {
                    input[k] = data.get(i)[k];
                }
                for (int l = 0; l < target.colNum(); l = l + 1) {
                    input[l + colNum] = targetRow[l];
                }
                result.putRow(input);
            }
        }
        return result;
    }

    public Table selectiveCombine(Table target, String[] overlap) {
        Integer[] sameOwnIndex = getIndexes(overlap);
        Integer[] sameOtherIndex = target.getIndexes(overlap);
        Integer[] differentOwnIndex = getIndexes(reducer(colNames, overlap));
        Integer[] differentOtherIndex = target.getIndexes(reducer(target.colNames, overlap));

        String[] newCol = new String[sameOwnIndex.length + differentOwnIndex.length + differentOtherIndex.length];
        String[] newColTypes = new String[newCol.length];
        for (int l = 0; l < sameOwnIndex.length; l = l + 1) {
            newCol[l] = colNames[sameOwnIndex[l]];
            newColTypes[l] = colTypes[sameOwnIndex[l]];
        }
        int a = 0;
        for (int k = sameOwnIndex.length; k < differentOwnIndex.length + sameOwnIndex.length; k = k + 1) {
            newCol[k] = colNames[differentOwnIndex[a]];
            newColTypes[k] = colTypes[differentOwnIndex[a]];
            a = a + 1;
        }
        int n = 0;
        for (int m = differentOwnIndex.length + sameOwnIndex.length;
             m < differentOtherIndex.length + differentOwnIndex.length + sameOwnIndex.length; m = m + 1) {
            newCol[m] = target.colNames()[differentOtherIndex[n]];
            newColTypes[m] = target.colTypes()[differentOtherIndex[n]];
            n = n + 1;
        }

        Table results = new Table(newCol, newColTypes);
        String[] input;
        int sameValue;
        for (int i = 0; i < size(); i = i + 1) {
            for (int j = 0; j < target.size(); j = j + 1) {
                sameValue = 0;
                for (int k = 0; k < overlap.length; k = k + 1) {
                    if (data.get(i)[sameOwnIndex[k]].equals(target.getRow(j)[sameOtherIndex[k]])) {
                        sameValue = sameValue + 1;
                    }
                }
                if (sameValue == overlap.length) {
                    input = new String[newCol.length];
                    for (int l = 0; l < sameOwnIndex.length; l = l + 1) {
                        input[l] = data.get(i)[sameOwnIndex[l]];
                    }
                    int b = 0;
                    for (int k = sameOwnIndex.length; k < differentOwnIndex.length + sameOwnIndex.length; k = k + 1) {
                        input[k] = data.get(i)[differentOwnIndex[b]];
                        b = b + 1;
                    }
                    int o = 0;
                    for (int m = differentOwnIndex.length + sameOwnIndex.length;
                         m < differentOtherIndex.length + differentOwnIndex.length + sameOwnIndex.length; m = m + 1) {
                        input[m] = target.getRow(j)[differentOtherIndex[o]];
                        o = o + 1;
                    }
                    results.putRow(input);
                }
            }
        }
        return results;
    }

    public Integer[] getIndexes(String[] wantedColumns) {
        Integer[] indexes = new Integer[wantedColumns.length];
        for (int i = 0; i < wantedColumns.length; i = i + 1) {
            for (int j = 0; j < colNum; j = j + 1) {
                if (wantedColumns[i].equals(colNames[j])) {
                    indexes[i] = j;
                }
            }
        }
        return indexes;
    }

    public Table selectByColumns(String[] wantedColumns) {
        String[] wantedColumnNames = new String[wantedColumns.length];
        String[] wantedColumnTypes = new String[wantedColumns.length];
        for (int l = 0; l < wantedColumns.length; l = l + 1) {
            if (!wantedColumns[l].contains(" as ")) {
                int ind = getIndex(whiteSpaceKiller(wantedColumns[l]));
                if (ind < 0) {
                    throw new RuntimeException("ERROR: Column name not found");
                }
                wantedColumnNames[l] = colNames[ind];
                wantedColumnTypes[l] = colTypes[ind];
            } else {
                String[] asColumn = wantedColumns[l].split(" as ");
                if (asColumn.length == 2) {
                    asColumn[0] = whiteSpaceKiller(asColumn[0]);
                    String[] separated;
                    String op = "";
                    separated = whiteSpaceKiller(asColumn[0]).split("\\+");
                    if (separated.length > 1) {
                        op = "+";
                    }
                    if (separated.length == 1) {
                        separated = separated[0].split("-");
                        op = "-";
                    }
                    if (separated.length == 1) {
                        separated = separated[0].split("\\*");
                        op = "*";
                    }
                    if (separated.length == 1) {
                        separated = separated[0].split("/");
                        op = "/";
                    }
                    if (separated.length == 1) {
                        int ind = getIndex(asColumn[0]);
                        if (ind == -1) {
                            throw new RuntimeException("ERROR: ");
                        }
                        wantedColumnTypes[l] = colTypes[ind];
                        wantedColumnNames[l] = asColumn[1];
                    } else {
                        int idx1 = getIndex(separated[0]);
                        if (idx1 == -1) {
                            throw new RuntimeException("ERROR: ");
                        }
                        String d1 = colTypes[idx1];
                        String d2;
                        int idx2 = getIndex(separated[1]);
                        if (idx2 == -1) {
                            d2 = dataType(separated[1]);
                        } else {
                            d2 = colTypes[idx2];
                        }
                        if (d1.equals("string") && d2.equals("string") && op.equals("+")) {
                            wantedColumnTypes[l] = "string";
                            wantedColumnNames[l] = asColumn[1];
                        } else if ((d1.equals("float") || d2.equals("float")) && !d1.equals("string") && !d2.equals("string")) {
                            wantedColumnTypes[l] = "float";
                            wantedColumnNames[l] = asColumn[1];
                        } else if (d1.equals("int") && d2.equals("int")) {
//                            if (op.equals("/")) {
//                                wantedColumnTypes[l] = "float";
//                            } else {
                            wantedColumnTypes[l] = "int";
//                            }
                            wantedColumnNames[l] = asColumn[1];
                        } else {
                            throw new RuntimeException("ERROR: malformed query");
                        }
                    }

                } else {
                    throw new RuntimeException("ERROR: malformed query");
                }
            }
        }
        Table results = new Table(wantedColumnNames, wantedColumnTypes);
        String[] input;
        for (int k = 0; k < counter; k = k + 1) {
            input = new String[wantedColumns.length];
            for (int m = 0; m < wantedColumns.length; m = m + 1) {
                if (!wantedColumns[m].contains(" as ")) {
                    int ind = getIndex(whiteSpaceKiller(wantedColumns[m]));
                    input[m] = data.get(k)[ind];
                } else {
                    String[] asColumn = wantedColumns[m].split(" as ");
                    if (asColumn.length == 2) {
                        asColumn[0] = whiteSpaceKiller(asColumn[0]);
                        String[] separated;
                        String op = "";
                        separated = whiteSpaceKiller(asColumn[0]).split("\\+");
                        if (separated.length > 1) {
                            op = "+";
                        }
                        if (separated.length == 1) {
                            separated = separated[0].split("-");
                            op = "-";
                        }
                        if (separated.length == 1) {
                            separated = separated[0].split("\\*");
                            op = "*";
                        }
                        if (separated.length == 1) {
                            separated = separated[0].split("/");
                            op = "/";
                        }
                        if (separated.length == 1) {
                            int ind = getIndex(asColumn[0]);
                            if (ind == -1) {
                                throw new RuntimeException();
                            }
                            input[m] = data.get(k)[ind];
                        } else {
                            int idx1 = getIndex(separated[0]);
                            if (idx1 == -1) {
                                throw new RuntimeException();
                            }
                            String d1 = colTypes[idx1];
                            String data1 = data.get(k)[idx1];
                            String d2, data2;
                            int idx2 = getIndex(separated[1]);
                            if (idx2 == -1) {
                                d2 = dataType(separated[1]);
                                data2 = separated[1];
                            } else {
                                d2 = colTypes[idx2];
                                data2 = data.get(k)[idx2];
                            }
                            if (data1.equals("NOVALUE")) {
                                if (d1.equals("string")) {
                                    data1 = "''";
                                } else if (d1.equals("int")) {
                                    data1 = "0";
                                } else if (d1.equals("float")) {
                                    data1 = "0.0";
                                }
                            }
                            if (data2.equals("NOVALUE")) {
                                if (d2.equals("string")) {
                                    data2 = "''";
                                } else if (d2.equals("int")) {
                                    data2 = "0";
                                } else if (d2.equals("float")) {
                                    data2 = "0.0";
                                }
                            }
                            if (d1.equals("string") && d2.equals("string") && op.equals("+")) {
                                input[m] = data1.substring(0, data1.length() - 1) + data2.substring(1);
                            } else if ((d1.equals("float") || d2.equals("float")) && !d1.equals("string") && !d2.equals("string")) {
                                if (((Float.parseFloat(data2) == 0.0) && op.equals("/")) || data1.equals("NaN") || data2.equals("NaN")) {
                                    input[m] = "NaN";
                                } else {
                                    input[m] = floatThreePointer(Float.toString(aritmaticOp(op, Float.parseFloat(data1), Float.parseFloat(data2))));
                                }
                            } else if (d1.equals("int") && d2.equals("int")) {
                                if (data1.equals("NaN") || data2.equals("NaN")) {
                                    input[m] = "NaN";
                                } else if (op.equals("/")) {
                                    if (Float.parseFloat(data2) == 0.0) {
                                        input[m] = "NaN";
                                    } else {
                                        input[m] = floatThreePointer(Float.toString(aritmaticOp(op, Float.parseFloat(data1), Float.parseFloat(data2)))).split("\\.")[0];
                                    }
                                } else {
                                    input[m] = Integer.toString(aritmaticOpI(op, Integer.parseInt(data1), Integer.parseInt(data2)));
                                }
                            } else {
                                throw new RuntimeException("ERROR: malformed query");
                            }
                        }

                    } else {
                        throw new RuntimeException("ERROR: malformed query");
                    }
                }
            }
            results.putRow(input);
        }
        return results;
    }

    public void putRow(String[] input) {
        for (int i = 0; i < input.length; i++) {
            input[i] = input[i].trim();
            if (colTypes[i].equals("int") || colTypes[i].equals("float")) {
                input[i] = leftZerosKiller(input[i]);
            }
        }
        this.data.put(counter, input);
        counter += 1;
    }

    private String leftZerosKiller(String input) {
        String[] temp = input.split("\\.");
        int a = temp[0].length() - 1;
        while (a > 0) {
            if (input.charAt(0) == '0' && a > 1) {
                input = input.substring(1);
                a = a - 1;
            } else {
                a = 0;
            }
        }
        return input;
    }

    public String printTableString() { //Prints table within function, no need to return anything
        String msg = "";

        for (int i = 0; i < colNames.length; i++) {
            msg += colNames[i] + " " + colTypes[i];

            if (i < colNames.length - 1) {
                msg += ",";
            }
        }

        msg += "\n";
        for (int i = 0; i < data.size(); i++) {
            String[] temp = data.get(i);
            for (int a = 0; a < colTypes.length; a++) {
                if (colTypes[a].equals("float")) {
                    msg += floatThreePointer(temp[a]);
                } else {
                    msg += temp[a];
                }
                if (a < colTypes.length - 1) {
                    msg += ",";
                }
            }
            msg += "\n";
        }
        return msg;
    }

    public Table selectByRows(String[] conditions) {
        String[][] temp = new String[conditions.length][3];
        for (int i = 0; i < conditions.length; i++) {
            String[] separated;
            String op = "";
            separated = whiteSpaceKiller(conditions[i]).split("==");
            if (separated.length > 1) {
                op = "==";
            }
            if (separated.length == 1) {
                separated = separated[0].split("!=");
                op = "!=";
            }
            if (separated.length == 1) {
                separated = separated[0].split(">=");
                op = ">=";
            }
            if (separated.length == 1) {
                separated = separated[0].split("<=");
                op = "<=";
            }
            if (separated.length == 1) {
                separated = separated[0].split(">");
                op = ">";
            }
            if (separated.length == 1) {
                separated = separated[0].split("<");
                op = "<";
            }
            if (separated.length == 1) {
                System.out.println(separated[0]);
                throw new RuntimeException("ERROR: Condition invalid");
            }
            temp[i][0] = separated[0];
            temp[i][1] = op;
            temp[i][2] = separated[1];
        }

        Table tmp = new Table(colNames(), colTypes());
        for (int i = 0; i < counter; i++) {
            boolean add = true;
            for (int j = 0; j < conditions.length; j++) {

                int idx1 = getIndex(temp[j][0]);
                if (idx1 == -1) {
                    throw new RuntimeException("ERROR: row not found");
                }
                int idx2 = getIndex(temp[j][2]);
                String type1 = colTypes[idx1];
                if (idx2 == -1) {
                    String type2 = dataType(temp[j][2]);
                    if (!checker(temp[j][1], data.get(i)[idx1], temp[j][2], type1, type2)) {
                        add = false;
                    }
                } else {
                    String type2 = colTypes[idx2];
                    if (!checker(temp[j][1], data.get(i)[idx1], data.get(i)[idx2], type1, type2)) {
                        add = false;
                    }
                }
            }
            if (add) {
                String[] input = new String[colNum()];
                for (int k = 0; k < colNum(); k++) {
                    input[k] = data.get(i)[k];
                }
                tmp.putRow(input);
            }
        }
        return tmp;

    }

    public int getIndex(String cName) {
        int unary = -1;
        for (int i = 0; i < colNum(); i++) {
            if (colNames[i].compareTo(cName) == 0) { //if there is a match in the colNames with the 2nd arg
                unary = i;
            }
        }
        return unary;
    }

    private boolean checker(String operator, String val1, String val2, String a1, String a2) {
        if (a1.equals("novalue") || a2.equals("novalue")) {
            return false;
        }
        if (a1.equals("string") && a2.equals("string")) { //check if string                                                    //FUNCTION MIKAEL
            if (!compString(operator, val1, val2)) {
                return false;
            }
        } else if ((a1.equals("float") || a2.equals("float")) && !a1.equals("string") && !a2.equals("string")) { //if either our column or the compare value is a float          //FUNCTION MIKAEL
            if (val1.equals("NaN") || val2.equals("NaN")) {
                return compNaN(operator, val1, val2);
            }
            if (!compFlo(operator, Float.parseFloat(val1), Float.parseFloat(val2))) {
                return false;
            }
        } else if (!a1.equals("string") && !a2.equals("string")) { // only ints
            if (val1.equals("NaN") || val2.equals("NaN")) {
                return compNaN(operator, val1, val2);
            }
            if (!compInt(operator, Integer.parseInt(val1), Integer.parseInt(val2))) {
                return false;
            }
        } else {
            throw new RuntimeException("ERROR: comparison data types");
        }
        return true;
    }

    private boolean compNaN(String relation, String value1, String value2) {
        if (value1.equals("NaN") && value2.equals("NaN")) {
            if (relation.equals("==") || relation.equals(">=") || relation.equals("<=")) {
                return true;
            } else {
                return false;
            }
        } else if (value1.equals("NaN")) {
            if (relation.equals(">") || relation.equals(">=") || relation.equals("!=")) {
                return true;
            } else {
                return false;
            }
        } else {
            if (relation.equals("<") || relation.equals("<=") || relation.equals("!=")) {
                return true;
            } else {
                return false;
            }
        }
    }

    private boolean compString(String relation, String value1, String value2) {
        int res = value1.compareTo(value2);
        switch (relation) {
            case "==":
                return res == 0;
            case "!=":
                return res != 0;
            case "<=":
                return res <= 0;
            case ">=":
                return res >= 0;
            case "<":
                return res < 0;
            case ">":
                return res > 0;
            default:
                throw new RuntimeException("ERROR: invalid relation");
        }
    }

    private boolean compInt(String relation, int value1, int value2) {

        switch (relation) {
            case "==":
                return value1 == value2;
            case "!=":
                return value1 != value2;
            case "<=":
                return value1 <= value2;
            case ">=":
                return value1 >= value2;
            case "<":
                return value1 < value2;
            case ">":
                return value1 > value2;
            default:
                throw new RuntimeException("ERROR: invalid relation");
        }

    }

    private boolean compFlo(String relation, Float value1, Float value2) {
        int res = Float.compare(value1, value2);
        switch (relation) {
            case "==":
                res = Float.compare(value1, value2);
                return res == 0;
            case "!=":
                return res != 0;
            case "<=":
                return res <= 0;
            case ">=":
                return res >= 0;
            case "<":
                return res < 0;
            case ">":
                return res > 0;
            default:
                throw new RuntimeException("ERROR: invalid relation");
        }

    }
}