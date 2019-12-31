/*
 * Copyright (c) 2018 - 2019 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package site.ycsb.db;

import static java.nio.charset.StandardCharsets.UTF_8;
import static kyotocabinet.DB.*;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;

// import kyotocabinet.*;

/**
 * Kyoto Cabinet binding for <a href="https://fallabs.com/kyotocabinet/">Kyoto Cabinet</a>.
 */
public class KyotocabinetDBClient extends DB {

  static final String PROPERTY_KYOTOCABINETDB_DIR = "/tmp/kyoto.dir";
  static final String PROPERTY_KYOTOCABINETDB_OPTIONS_FILE = "kyoto.optionsfile";
  // private static final String COLUMN_FAMILY_NAMES_FILENAME = "CF_NAMES";

  // private static final Logger LOGGER = LoggerFactory.getLogger(KyotocabinetDBClient.class);

  // @GuardedBy("KyotocabinetDBClient.class") private static Path kcDbDir = null;
  // @GuardedBy("KyotocabinetDBClient.class") private static Path optionsFile = null;
  // // @GuardedBy("KyotocabinetDBClient.class") private static RocksObject dbOptions = null;
  // @GuardedBy("KyotocabinetDBClient.class") private static kyotocabinet.DB  kcDb = null;
  // @GuardedBy("KyotocabinetDBClient.class") private static int references = 0;

  private static Path kcDbDir = null;
  private static Path optionsFile = null;
  private static kyotocabinet.DB  kcDb = null;
  private static int references = 0;


  @Override
  public void init() throws DBException {
    synchronized(KyotocabinetDBClient.class) {
      if(kcDb == null) {
        kcDbDir = Paths.get(PROPERTY_KYOTOCABINETDB_DIR);
        // LOGGER.info("RocksDB data dir: " + kcDbDir);

        String optionsFileString = PROPERTY_KYOTOCABINETDB_OPTIONS_FILE;
        if (optionsFileString != null) {
          optionsFile = Paths.get(optionsFileString);
          // LOGGER.info("RocksDB options file: " + optionsFile);
        }

        // try {
        //   if (optionsFile != null) {
        //     kcDb = initRocksDBWithOptionsFile();
        //   } else {
        //     kcDb = initRocksDB();
        //   }
        // } catch (final IOException | RocksDBException e) {
        //   throw new DBException(e);
        // }
        kcDb = new kyotocabinet.DB();

        if (!kcDb.open(kcDbDir.toString(), OREADER | OCREATE | OWRITER)) {
          System.err.println("open error: " + kcDb.error());
        }
      }

      references++;
    }
  }

  /**
   * Initializes and opens the RocksDB database.
   *
   * Should only be called with a {@code synchronized(KyotocabinetDBClient.class)` block}.
   *
   * @return The initialized and open RocksDB instance.
   */
  /*
  private RocksDB initRocksDBWithOptionsFile() throws IOException, RocksDBException {
    if(!Files.exists(kcDbDir)) {
      Files.createDirectories(kcDbDir);
    }

    final DBOptions options = new DBOptions();
    final List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
    final List<ColumnFamilyHandle> cfHandles = new ArrayList<>();

    RocksDB.loadLibrary();
    OptionsUtil.loadOptionsFromFile(optionsFile.toAbsolutePath().toString(), Env.getDefault(), options, cfDescriptors);
    dbOptions = options;

    final RocksDB db = RocksDB.open(options, kcDbDir.toAbsolutePath().toString(), cfDescriptors, cfHandles);

    for(int i = 0; i < cfDescriptors.size(); i++) {
      String cfName = new String(cfDescriptors.get(i).getName());
      final ColumnFamilyHandle cfHandle = cfHandles.get(i);
      final ColumnFamilyOptions cfOptions = cfDescriptors.get(i).getOptions();

      COLUMN_FAMILIES.put(cfName, new ColumnFamily(cfHandle, cfOptions));
    }

    return db;
  }
  */

  /**
   * Initializes and opens the RocksDB database.
   *
   * Should only be called with a {@code synchronized(KyotocabinetDBClient.class)` block}.
   *
   * @return The initialized and open RocksDB instance.
   */
  /*
  private RocksDB initRocksDB() throws IOException, RocksDBException {
    if(!Files.exists(kcDbDir)) {
      Files.createDirectories(kcDbDir);
    }

    final List<String> cfNames = loadColumnFamilyNames();
    final List<ColumnFamilyOptions> cfOptionss = new ArrayList<>();
    final List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();

    for(final String cfName : cfNames) {
      final ColumnFamilyOptions cfOptions = new ColumnFamilyOptions()
          .optimizeLevelStyleCompaction();
      final ColumnFamilyDescriptor cfDescriptor = new ColumnFamilyDescriptor(
          cfName.getBytes(UTF_8),
          cfOptions
      );
      cfOptionss.add(cfOptions);
      cfDescriptors.add(cfDescriptor);
    }

    final int rocksThreads = Runtime.getRuntime().availableProcessors() * 2;

    if(cfDescriptors.isEmpty()) {
      final Options options = new Options()
          .optimizeLevelStyleCompaction()
          .setCreateIfMissing(true)
          .setCreateMissingColumnFamilies(true)
          .setIncreaseParallelism(rocksThreads)
          .setMaxBackgroundCompactions(rocksThreads)
          .setInfoLogLevel(InfoLogLevel.INFO_LEVEL);
      dbOptions = options;
      return RocksDB.open(options, kcDbDir.toAbsolutePath().toString());
    } else {
      final DBOptions options = new DBOptions()
          .setCreateIfMissing(true)
          .setCreateMissingColumnFamilies(true)
          .setIncreaseParallelism(rocksThreads)
          .setMaxBackgroundCompactions(rocksThreads)
          .setInfoLogLevel(InfoLogLevel.INFO_LEVEL);
      dbOptions = options;

      final List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
      final RocksDB db = RocksDB.open(options, kcDbDir.toAbsolutePath().toString(), cfDescriptors, cfHandles);
      for(int i = 0; i < cfNames.size(); i++) {
        COLUMN_FAMILIES.put(cfNames.get(i), new ColumnFamily(cfHandles.get(i), cfOptionss.get(i)));
      }
      return db;
    }
  }

  */
  @Override
  public void cleanup() throws DBException {
    super.cleanup();

    synchronized (KyotocabinetDBClient.class) {
      try {
        if (references == 1) {
          // for (final ColumnFamily cf : COLUMN_FAMILIES.values()) {
          //   cf.getHandle().close();
          // }

          kcDb.close();
          kcDb = null;

          // dbOptions.close();
          // dbOptions = null;

          // for (final ColumnFamily cf : COLUMN_FAMILIES.values()) {
          //   cf.getOptions().close();
          // }
          // saveColumnFamilyNames();
          // COLUMN_FAMILIES.clear();

          kcDbDir = null;
        }

      // } catch (final IOException e) {
      //   throw new DBException(e);
      } finally {
        references--;
      }
    }
  }

  @Override
  public Status read(final String table, final String key, final Set<String> fields,
      final Map<String, ByteIterator> result) {
    try {
      // ignore table. There is no table in KCDB
      final byte[] values = kcDb.get(key.getBytes(UTF_8));
      if(values == null) {
        return Status.NOT_FOUND;
      }
      deserializeValues(values, fields, result);
      return Status.OK;
    } catch(final kyotocabinet.Error e) {
      // LOGGER.error(e.name(), e);
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(final String table, final String startkey, final int recordcount, final Set<String> fields,
        final Vector<HashMap<String, ByteIterator>> result) {
    // try {
    //   // ignore table. There is no table in KCDB
    //   // if (!COLUMN_FAMILIES.containsKey(table)) {
    //   //   createColumnFamily(table);
    //   // }

    //   // final ColumnFamilyHandle cf = COLUMN_FAMILIES.get(table).getHandle();
    //   final kyotocabinet.ValueIterator vi = ;
    //   {
    //     int iterations = 0;
    //     for (iterator.seek(startkey.getBytes(UTF_8)); iterator.isValid() && iterations < recordcount;
    //          iterator.next()) {
    //       final HashMap<String, ByteIterator> values = new HashMap<>();
    //       deserializeValues(iterator.value(), fields, values);
    //       result.add(values);
    //       iterations++;
    //     }
    //   }

    //   return Status.OK;
    // } catch(final RocksDBException e) {
    //   LOGGER.error(e.getMessage(), e);
    //   return Status.ERROR;
    // }
    throw new UnsupportedOperationException();
  }

  @Override
  public Status update(final String table, final String key, final Map<String, ByteIterator> values) {
    //TODO(AR) consider if this would be faster with merge operator

    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      // System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());
      kcDb.set(entry.getKey(), entry.getValue().toString());
    }
    return Status.OK;

    // try {
    //   if (!COLUMN_FAMILIES.containsKey(table)) {
    //     createColumnFamily(table);
    //   }

    //   final ColumnFamilyHandle cf = COLUMN_FAMILIES.get(table).getHandle();
    //   final Map<String, ByteIterator> result = new HashMap<>();
    //   final byte[] currentValues = kcDb.get(cf, key.getBytes(UTF_8));
    //   if(currentValues == null) {
    //     return Status.NOT_FOUND;
    //   }
    //   deserializeValues(currentValues, null, result);

    //   //update
    //   result.putAll(values);

    //   //store
    //   kcDb.put(cf, key.getBytes(UTF_8), serializeValues(result));

    //   return Status.OK;

    // } catch(final RocksDBException | IOException e) {
    //   LOGGER.error(e.getMessage(), e);
    //   return Status.ERROR;
    // }
  }

  @Override
  public Status insert(final String table, final String key, final Map<String, ByteIterator> values) {
    /*try {
      if (!COLUMN_FAMILIES.containsKey(table)) {
        createColumnFamily(table);
      }

      final ColumnFamilyHandle cf = COLUMN_FAMILIES.get(table).getHandle();
      kcDb.put(cf, key.getBytes(UTF_8), serializeValues(values));

      return Status.OK;
    } catch(final RocksDBException | IOException e) {
      LOGGER.error(e.getMessage(), e);
      return Status.ERROR;
    }*/
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      // toInsert.put(entry.getKey(), entry.getValue().toArray());
      kcDb.set(entry.getKey(), entry.getValue().toString());
    }

    return Status.OK;
  }

  @Override
  public Status delete(final String table, final String key) {
    // try {
    //   if (!COLUMN_FAMILIES.containsKey(table)) {
    //     createColumnFamily(table);
    //   }

    //   final ColumnFamilyHandle cf = COLUMN_FAMILIES.get(table).getHandle();
    //   kcDb.delete(cf, key.getBytes(UTF_8));

    //   return Status.OK;
    // } catch(final RocksDBException e) {
    //   LOGGER.error(e.getMessage(), e);
    //   return Status.ERROR;
    // }

    kcDb.remove(key);
    return Status.OK;
  }
/*
  private void saveColumnFamilyNames() throws IOException {
    final Path file = kcDbDir.resolve(COLUMN_FAMILY_NAMES_FILENAME);
    try(final PrintWriter writer = new PrintWriter(Files.newBufferedWriter(file, UTF_8))) {
      writer.println(new String(RocksDB.DEFAULT_COLUMN_FAMILY, UTF_8));
      for(final String cfName : COLUMN_FAMILIES.keySet()) {
        writer.println(cfName);
      }
    }
  }

  private List<String> loadColumnFamilyNames() throws IOException {
    final List<String> cfNames = new ArrayList<>();
    final Path file = kcDbDir.resolve(COLUMN_FAMILY_NAMES_FILENAME);
    if(Files.exists(file)) {
      try (final LineNumberReader reader =
        new LineNumberReader(Files.newBufferedReader(file, UTF_8))) {
        String line = null;
        while ((line = reader.readLine()) != null) {
          cfNames.add(line);
        }
      }
    }
    return cfNames;
  }
  */
  private Map<String, ByteIterator> deserializeValues(final byte[] values, final Set<String> fields,
            final Map<String, ByteIterator> result) {
    final ByteBuffer buf = ByteBuffer.allocate(4);
    
    int offset = 0;
    while(offset < values.length) {
      buf.put(values, offset, 4);
      buf.flip();
      final int keyLen = buf.getInt();
      buf.clear();
      offset += 4;
      
      final String key = new String(values, offset, keyLen);
      offset += keyLen;
      
      buf.put(values, offset, 4);
      buf.flip();
      final int valueLen = buf.getInt();
      buf.clear();
      offset += 4;
      
      if(fields == null || fields.contains(key)) {
        result.put(key, new ByteArrayByteIterator(values, offset, valueLen));
      }
      
      offset += valueLen;
    }
    
    return result;
  }
}
  /*
  private byte[] serializeValues(final Map<String, ByteIterator> values) throws IOException {
    try(final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      final ByteBuffer buf = ByteBuffer.allocate(4);

      for(final Map.Entry<String, ByteIterator> value : values.entrySet()) {
        final byte[] keyBytes = value.getKey().getBytes(UTF_8);
        final byte[] valueBytes = value.getValue().toArray();

        buf.putInt(keyBytes.length);
        baos.write(buf.array());
        baos.write(keyBytes);

        buf.clear();

        buf.putInt(valueBytes.length);
        baos.write(buf.array());
        baos.write(valueBytes);

        buf.clear();
      }
      return baos.toByteArray();
    }
  }

  /*

  private ColumnFamilyOptions getDefaultColumnFamilyOptions(final String destinationCfName) {
    final ColumnFamilyOptions cfOptions;

    if (COLUMN_FAMILIES.containsKey("default")) {
      LOGGER.warn("no column family options for \"" + destinationCfName + "\" " +
                  "in options file - using options from \"default\"");
      cfOptions = COLUMN_FAMILIES.get("default").getOptions();
    } else {
      LOGGER.warn("no column family options for either \"" + destinationCfName + "\" or " +
                  "\"default\" in options file - initializing with empty configuration");
      cfOptions = new ColumnFamilyOptions();
    }
    LOGGER.warn("Add a CFOptions section for \"" + destinationCfName + "\" to the options file, " +
                "or subsequent runs on this DB will fail.");

    return cfOptions;
  }

  private void createColumnFamily(final String name) throws RocksDBException {
    COLUMN_FAMILY_LOCKS.putIfAbsent(name, new ReentrantLock());

    final Lock l = COLUMN_FAMILY_LOCKS.get(name);
    l.lock();
    try {
      if(!COLUMN_FAMILIES.containsKey(name)) {
        final ColumnFamilyOptions cfOptions;

        if (optionsFile != null) {
          // RocksDB requires all options files to include options for the "default" column family;
          // apply those options to this column family
          cfOptions = getDefaultColumnFamilyOptions(name);
        } else {
          cfOptions = new ColumnFamilyOptions().optimizeLevelStyleCompaction();
        }

        final ColumnFamilyHandle cfHandle = kcDb.createColumnFamily(
            new ColumnFamilyDescriptor(name.getBytes(UTF_8), cfOptions)
        );
        COLUMN_FAMILIES.put(name, new ColumnFamily(cfHandle, cfOptions));
      }
    } finally {
      l.unlock();
    }
  }

  private static final class ColumnFamily {
    private final ColumnFamilyHandle handle;
    private final ColumnFamilyOptions options;

    private ColumnFamily(final ColumnFamilyHandle handle, final ColumnFamilyOptions options) {
      this.handle = handle;
      this.options = options;
    }

    public ColumnFamilyHandle getHandle() {
      return handle;
    }

    public ColumnFamilyOptions getOptions() {
      return options;
    }
  }
}
*/