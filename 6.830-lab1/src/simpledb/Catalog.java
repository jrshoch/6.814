package simpledb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas. For now, this is a stub catalog that must be populated
 * with tables by a user program before it can be used -- eventually, this
 * should be converted to a catalog that reads a catalog table from disk.
 * 
 * @Threadsafe
 */
public class Catalog {

  private final List<DbFile> files;
  private final List<String> tableNames;
  private final List<String> primaryKeyFields;
  private final Map<String, Integer> nameToIdMap;
  private final Map<Integer, Integer> idToIndexMap;

  /**
   * Constructor. Creates a new, empty catalog.
   */
  public Catalog() {
    this.files = Lists.newArrayList();
    this.tableNames = Lists.newArrayList();
    this.primaryKeyFields = Lists.newArrayList();
    this.nameToIdMap = Maps.newHashMap();
    this.idToIndexMap = Maps.newHashMap();
  }

  /**
   * Add a new table to the catalog. This table's contents are stored in the
   * specified DbFile.
   * 
   * @param file the contents of the table to add; file.getId() is the identfier
   *          of this file/tupledesc param for the calls getTupleDesc and
   *          getFile
   * @param name the name of the table -- may be an empty string. May not be
   *          null. If a name
   * @param pkeyField the name of the primary key field conflict exists, use the
   *          last table to be added as the table for a given name.
   */
  public void addTable(DbFile file, String tableName, String pkeyField) {
    Integer id = new Integer(file.getId());
    nameToIdMap.put(tableName, id);
    idToIndexMap.put(id, new Integer(files.size()));
    this.files.add(file);
    this.tableNames.add(tableName);
    this.primaryKeyFields.add(pkeyField);
  }

  public void addTable(DbFile file, String name) {
    addTable(file, name, "");
  }

  /**
   * Add a new table to the catalog. This table has tuples formatted using the
   * specified TupleDesc and its contents are stored in the specified DbFile.
   * 
   * @param file the contents of the table to add; file.getId() is the identfier
   *          of this file/tupledesc param for the calls getTupleDesc and
   *          getFile
   */
  public void addTable(DbFile file) {
    addTable(file, (UUID.randomUUID()).toString());
  }

  /**
   * Return the id of the table with a specified name,
   * 
   * @throws NoSuchElementException if the table doesn't exist
   */
  public int getTableId(String tableName) throws NoSuchElementException {
    if (nameToIdMap.containsKey(tableName)) {
      return nameToIdMap.get(tableName).intValue();
    }
    throw new NoSuchElementException();
  }
  
  private void checkId(int tableId) throws NoSuchElementException {
    if (!idToIndexMap.containsKey(new Integer(tableId))) {
      throw new NoSuchElementException();
    }
  }
  
  private int getIndex(int tableId) throws NoSuchElementException {
    checkId(tableId);
    return idToIndexMap.get(new Integer(tableId)).intValue();
  }

  /**
   * Returns the tuple descriptor (schema) of the specified table
   * 
   * @param tableid The id of the table, as specified by the DbFile.getId()
   *          function passed to addTable
   * @throws NoSuchElementException if the table doesn't exist
   */
  public TupleDesc getTupleDesc(int tableId) throws NoSuchElementException {
    return files.get(getIndex(tableId)).getTupleDesc();
  }

  /**
   * Returns the DbFile that can be used to read the contents of the specified
   * table.
   * 
   * @param tableid The id of the table, as specified by the DbFile.getId()
   *          function passed to addTable
   */
  public DbFile getDatabaseFile(int tableId) throws NoSuchElementException {
    return files.get(getIndex(tableId));
  }

  public String getPrimaryKey(int tableId) {
    return primaryKeyFields.get(getIndex(tableId));
  }

  protected List<DbFile> getFiles() {
    return files;
  }

  public Iterator<Integer> tableIdIterator() {
    return new Iterator<Integer>() {
      
      private int index = 0;

      @Override
      public boolean hasNext() {
        return index < getFiles().size();
      }

      @Override
      public Integer next() {
        Integer id = new Integer(getFiles().get(index).getId());
        index += 1;
        return id;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
      
    };
  }

  public String getTableName(int tableId) {
    return tableNames.get(getIndex(tableId));
  }

  /** Delete all tables from the catalog */
  public void clear() {
    files.clear();
    tableNames.clear();
    primaryKeyFields.clear();
    nameToIdMap.clear();
  }

  /**
   * Reads the schema from a file and creates the appropriate tables in the
   * database.
   * 
   * @param catalogFile
   */
  public void loadSchema(String catalogFile) {
    String line = "";
    String baseFolder = new File(new File(catalogFile).getAbsolutePath()).getParent();
    try {
      BufferedReader br = new BufferedReader(new FileReader(new File(catalogFile)));

      while ((line = br.readLine()) != null) {
        // assume line is of the format name (field type, field type, ...)
        String name = line.substring(0, line.indexOf("(")).trim();
        // System.out.println("TABLE NAME: " + name);
        String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
        String[] els = fields.split(",");
        ArrayList<String> names = new ArrayList<String>();
        ArrayList<Type> types = new ArrayList<Type>();
        String primaryKey = "";
        for (String e : els) {
          String[] els2 = e.trim().split(" ");
          names.add(els2[0].trim());
          if (els2[1].trim().toLowerCase().equals("int"))
            types.add(Type.INT_TYPE);
          else if (els2[1].trim().toLowerCase().equals("string"))
            types.add(Type.STRING_TYPE);
          else {
            System.out.println("Unknown type " + els2[1]);
            System.exit(0);
          }
          if (els2.length == 3) {
            if (els2[2].trim().equals("pk"))
              primaryKey = els2[0].trim();
            else {
              System.out.println("Unknown annotation " + els2[2]);
              System.exit(0);
            }
          }
        }
        Type[] typeAr = types.toArray(new Type[0]);
        String[] namesAr = names.toArray(new String[0]);
        TupleDesc t = new TupleDesc(typeAr, namesAr);
        HeapFile tabHf = new HeapFile(new File(baseFolder + "/" + name + ".dat"), t);
        addTable(tabHf, name, primaryKey);
        System.out.println("Added table : " + name + " with schema " + t);
      }
    } catch (IOException e) {
      e.printStackTrace();
      System.exit(0);
    } catch (IndexOutOfBoundsException e) {
      System.out.println("Invalid catalog entry : " + line);
      System.exit(0);
    }
  }
}
