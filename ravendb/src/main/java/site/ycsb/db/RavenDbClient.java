package site.ycsb.db;

import net.ravendb.client.documents.DocumentStore;
import net.ravendb.client.serverwide.DatabaseRecord;
import net.ravendb.client.serverwide.operations.CreateDatabaseOperation;
import net.ravendb.client.serverwide.operations.GetDatabaseNamesOperation;
import okhttp3.*;
import org.json.JSONArray;
import org.json.JSONObject;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * RavenDB binding for YCSB framework.
 */
public class RavenDbClient extends DB {

  private static OkHttpClient CLIENT;

  private static String url;
  private static String databaseName;
  private final List<String> batchInserts = new ArrayList<>();
  private static boolean debug = false;

  /**
   * The batch size to use for inserts.
   */
  private static int batchSize;

  private static final AtomicBoolean DB_INIT_COMPLETE = new AtomicBoolean();
  private static final MediaType MEDIA_TYPE = MediaType.parse("application/json");

  /**
   * Delete a record from the database.
   *
   * @param table The name of the table
   * @param key   The record key of the record to delete.
   * @return Zero on success, a non-zero error code on error. See the {@link DB}
   * class's description for a discussion of error codes.
   */
  @Override
  public Status delete(String table, String key) {
    String path = "/docs?id=" + key;
    Request request = requestBuilder(null, "DELETE", path);
    try (Response response = CLIENT.newCall(request).execute()) {
      if (response.isSuccessful()) {
        return Status.OK;
      } else {
        System.err.println(Objects.requireNonNull(response.body()).string());
        return Status.ERROR;
      }
    } catch (IOException e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  /**
   * Initialize any state for this DB. Called once per DB instance; there is one
   * DB instance per client thread.
   */
  @Override
  public void init() {
    synchronized (DB_INIT_COMPLETE) {
      if (!DB_INIT_COMPLETE.get()) {
        Properties props = getProperties();
        // Set insert batchsize, default 1 - to be YCSB-original equivalent
        batchSize = Integer.parseInt(props.getProperty("batchsize", "1"));
        debug = Boolean.parseBoolean(props.getProperty("debug", "false"));
        databaseName = props.getProperty("ravendb.dbname", "ycsb");
        url = props.getProperty("ravendb.host", null);
        String port = props.getProperty("port", "8080");
        int maxRequests = Integer.parseInt(props.getProperty("ravendb.maxRequests", "1000"));
        int maxRequestsPerHost = Integer.parseInt(props.getProperty("ravendb.maxRequestsPerHost", "100"));
        // TODO: implement ChangeVector if optimistic concurrency is enabled, requires the Database ID
        //  Get DatabaseId through an HTTP request with a query
        //  Add a HashMap to save the current ChangeVector for each ID
//        boolean concurrency = Boolean.parseBoolean(props.getProperty("useOptimisticConcurrency", "false"));
        url = "http://" + url + ":" + port;
        Dispatcher dispatcher = new Dispatcher();
        dispatcher.setMaxRequests(maxRequests);
        dispatcher.setMaxRequestsPerHost(maxRequestsPerHost);
        CLIENT = new OkHttpClient().newBuilder().dispatcher(dispatcher).build();
        DocumentStore store =
            new DocumentStore(url, databaseName);
        store.initialize();
        GetDatabaseNamesOperation operation = new GetDatabaseNamesOperation(0, 25);
        String[] databaseNames = store.maintenance().server().send(operation);
        List<String> list = Arrays.asList(databaseNames);
        if (!list.contains(databaseName)) {
          DatabaseRecord databaseRecord = new DatabaseRecord();
          databaseRecord.setDatabaseName(databaseName);
          store.maintenance().server().send(new CreateDatabaseOperation(databaseRecord));
        }
        store.close();
        DB_INIT_COMPLETE.set(true);
        System.out.println("Setup completed with URL: " + url);
      }
    }
  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to insert.
   * @param values A HashMap of field/value pairs to insert in the record
   * @return Zero on success, a non-zero error code on error. See the {@link DB}
   * class's description for a discussion of error codes.
   */
  @Override
  public Status insert(String table, String key,
                       Map<String, ByteIterator> values) {
    String httpMethod = "POST";
    String path = "/bulk_docs";
    String records = "";
    if (batchSize == 1) {
      records += "{\n\"Commands\": [";
      StringJoiner batchRecord = new StringJoiner(",\n");
      batchRecord.add(batchedInsertBuilder(values, key));
      records += batchRecord + "\n]\n}";
    } else {
      batchInserts.add(batchedInsertBuilder(values, key));
      if (batchInserts.size() == batchSize) {
        records += "{\n\"Commands\": [";
        StringJoiner batchRecord = new StringJoiner(",\n");
        for (String record : batchInserts) {
          batchRecord.add(record);
        }
        records += batchRecord + "\n]\n}";
        batchInserts.clear();
      } else {
        return Status.BATCHED_OK;
      }
    }

    RequestBody insertBody = RequestBody.create(records, MEDIA_TYPE);
    Request request = requestBuilder(insertBody, httpMethod, path);
    try (Response response = CLIENT.newCall(request).execute()) {
      if (response.isSuccessful()) {
        return Status.OK;
      } else {
        System.err.println(Objects.requireNonNull(response.body()).string());
        return Status.ERROR;
      }
    } catch (IOException e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  /**
   * Read a record from the database. Each field/value pair from the result will
   * be stored in a HashMap.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to read.
   * @param fields The list of fields to read, or null for all of them
   * @param result A HashMap of field/value pairs for the result
   * @return Zero on success, a non-zero error code on error or "not found".
   */
  @Override
  public Status read(String table, String key, Set<String> fields,
                     Map<String, ByteIterator> result) {
    String path = "/docs?id=" + key;
    Request request = requestBuilder(null, "GET", path);
    try (Response response = CLIENT.newCall(request).execute()) {
      if (response.isSuccessful()) {
        if (debug) {
          JSONObject json = new JSONObject(Objects.requireNonNull(response.body()).string());
          JSONArray jsonArray = json.getJSONArray("Results");
          fillMap(result, jsonArray);
        }
        return Status.OK;
      } else {
        System.err.println(Objects.requireNonNull(response.body()).string());
        return Status.ERROR;
      }
    } catch (IOException e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  /**
   * Perform a range scan for a set of records in the database. Each field/value
   * pair from the result will be stored in a HashMap.
   *
   * @param table       The name of the table
   * @param startkey    The record key of the first record to read.
   * @param recordcount The number of records to read
   * @param fields      The list of fields to read, or null for all of them
   * @param result      A Vector of HashMaps, where each HashMap is a set field/value
   *                    pairs for one record
   * @return Zero on success, a non-zero error code on error. See the {@link DB}
   * class's description for a discussion of error codes.
   */
  @Override
  public Status scan(String table, String startkey, int recordcount,
                     Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
//    TODO Use numerical key instead, as the scan uses lexical ordering, so for
//     http://localhost:8080/databases/benchdb/docs?startsWith=user&startAfter=user3995&pageSize=5"  user3996, user3997,
//     user3998, user3999, user4 are in the result
    StringBuilder path = new StringBuilder("docs?startsWith=user/&startAfter="
        + startkey + "&pageSize=" + recordcount);
    Request request = requestBuilder(null, "GET", path.toString());
    try (Response response = CLIENT.newCall(request).execute()) {
      if (response.isSuccessful()) {
        if (debug) {
          JSONObject json = new JSONObject(Objects.requireNonNull(response.body()).string());
          JSONArray jsonArray = json.getJSONArray("Results");
          for (int i = 0; i < jsonArray.length(); i++) {
            HashMap<String, ByteIterator> resultMap = new HashMap<String, ByteIterator>();
            fillMap(resultMap, jsonArray);
            result.add(resultMap);
          }
        }
        return Status.OK;
      } else {
        System.err.println(Objects.requireNonNull(response.body()).string());
        return Status.ERROR;
      }
    } catch (IOException e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  /**
   * Update a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key, overwriting any existing values with the same field name.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to write.
   * @param values A HashMap of field/value pairs to update in the record
   * @return Zero on success, a non-zero error code on error. See this class's
   * description for a discussion of error codes.
   */
  @Override
  public Status update(String table, String key,
                       Map<String, ByteIterator> values) {
    String httpMethod = "POST";
    String path = "/bulk_docs";
    String records = "";
    records += "{\n\"Commands\": [";
    StringJoiner batchRecord = new StringJoiner(",\n");
    batchRecord.add(batchedUpdateBuilder(values, key));
    records += batchRecord + "\n]\n}";

    RequestBody insertBody = RequestBody.create(records, MEDIA_TYPE);
    Request request = requestBuilder(insertBody, httpMethod, path);
    try (Response response = CLIENT.newCall(request).execute()) {
      if (response.isSuccessful()) {
        return Status.OK;
      } else {
        System.err.println(Objects.requireNonNull(response.body()).string());
        return Status.ERROR;
      }
    } catch (IOException e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  private String insertBuilder(Map<String, ByteIterator> values) {
    StringBuilder records = new StringBuilder(" {\n");

    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      String value = entry.getValue().toString();
      // Escape backslash and quotation marks to get valid json
      value = value.replaceAll("\\\\", "\\\\\\\\");
      value = value.replaceAll("\"", "\\\\\"");
      records.append("    \"").append(entry.getKey()).append("\": \"").append(value).append("\",\n");
    }
    records.append("   \"@metadata\":{\n");
    records.append("   \"@collection\":\"records\"\n");
    records.append("   }\n");
    records.append("}");
    return records.toString();
  }

  private String batchedInsertBuilder(Map<String, ByteIterator> values, String key) {
    return " {\n" + "   \"Id\":\"" + key + "\",\n" +
        "   \"ChangeVector\": null,\n" +
        "   \"Document\":" +
        insertBuilder(values) +
        "   \"Type\": \"PUT\"\n" +
        "}";
  }

  private String batchedUpdateBuilder(Map<String, ByteIterator> values, String key) {
    return " {\n" + "   \"Id\":\"" + key + "\",\n" +
        "   \"ChangeVector\": null,\n" +
        "   \"Document\":" +
        insertBuilder(values) +
        "   \"Type\": \"PUT\"\n" +
        "}";
  }

  //TODO implement authorization
  private Request requestBuilder(RequestBody body, String method, String path) {
    return new Request.Builder()
        .url(url + "/databases/" + databaseName + path)
        .method(method, body)
        .addHeader("Content-Type", "application/json")
        .build();
  }

  protected void fillMap(Map<String, ByteIterator> resultMap, JSONArray jsonArray) {
    for (Object o : jsonArray) {
      if (!JSONObject.NULL.equals(o)) {
        JSONObject jsonLineItem = (JSONObject) o;
        for (String key : jsonLineItem.keySet()) {
          if (key.equals("id") || key.contains("field")) {
            resultMap.put(key, new StringByteIterator(jsonLineItem.get(key).toString()));
          }
        }
      }
    }
  }
}
