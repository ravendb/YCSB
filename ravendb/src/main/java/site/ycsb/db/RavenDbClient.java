package site.ycsb.db;

import net.ravendb.client.documents.DocumentStore;
import net.ravendb.client.serverwide.DatabaseRecord;
import net.ravendb.client.serverwide.operations.CreateDatabaseOperation;
import net.ravendb.client.serverwide.operations.GetDatabaseNamesOperation;
import okhttp3.*;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * RavenDB binding for YCSB framework.
 */
public class RavenDbClient extends DB {

  private OkHttpClient client;

  private static String url;
  private static String databaseName;
  private final List<String> batchInserts = new ArrayList<>();
  private static boolean debug = false;
  private static HashMap<String, String> changeVectors = new HashMap<>();
  private static boolean concurrency;
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
    try (Response response = client.newCall(request).execute()) {
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

      Properties props = getProperties();
      int maxRequests = Integer.parseInt(props.getProperty("ravendb.maxRequests", "100"));
      int maxRequestsPerHost = Integer.parseInt(props.getProperty("ravendb.maxRequestsPerHost", "100"));
      Dispatcher dispatcher = new Dispatcher();
      dispatcher.setMaxRequests(maxRequests);
      dispatcher.setMaxRequestsPerHost(maxRequestsPerHost);
      client = new OkHttpClient().newBuilder().dispatcher(dispatcher).build();
      if (!DB_INIT_COMPLETE.get()) {
        // Set insert batchsize, default 1 - to be YCSB-original equivalent
        batchSize = Integer.parseInt(props.getProperty("batchsize", "1"));
        debug = Boolean.parseBoolean(props.getProperty("debug", "false"));
        databaseName = props.getProperty("ravendb.dbname", "ycsb");
        url = props.getProperty("ravendb.host", null);
        String port = props.getProperty("port", "8080");
        concurrency = Boolean.parseBoolean(props.getProperty("useOptimisticConcurrency", "false"));
        url = "http://" + url + ":" + port;

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
        System.out.println("maxRequests is set to " + maxRequests +
            " and maxRequestsPerHost is set to " + maxRequestsPerHost);
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
      records += "{\"Commands\":[";
      StringJoiner batchRecord = new StringJoiner(",");
      batchRecord.add(batchedInsertBuilder(values, key));
      records += batchRecord + "]}";
    } else {
      batchInserts.add(batchedInsertBuilder(values, key));
      if (batchInserts.size() == batchSize) {
        records += "{\"Commands\":[";
        StringJoiner batchRecord = new StringJoiner(",");
        for (String record : batchInserts) {
          batchRecord.add(record);
        }
        records += batchRecord + "]}";
        batchInserts.clear();
      } else {
        return Status.BATCHED_OK;
      }
    }


    RequestBody insertBody = RequestBody.create(records, MEDIA_TYPE);
    Request request = requestBuilder(insertBody, httpMethod, path);
    try (Response response = client.newCall(request).execute()) {
      if (response.isSuccessful()) {
        if (concurrency) {
          JSONObject json = new JSONObject(Objects.requireNonNull(response.body()).string());
          JSONArray jsonArray = json.getJSONArray("Results");
          writeChangeVectorWrite(jsonArray);
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
    try (Response response = client.newCall(request).execute()) {
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
    // the scan uses lexical ordering, so for
    // http://localhost:8080/databases/benchdb/docs?startsWith=user&startAfter=user3995&pageSize=5"  user3996, user3997,
    // user3998, user3999, user4 are in the result
    String path = "/docs?startsWith=user/&startAfter=" + startkey + "&pageSize=" + recordcount;
    Request request = requestBuilder(null, "GET", path);
    try (Response response = client.newCall(request).execute()) {
      if (response.isSuccessful()) {
        if (debug) {
          JSONObject json = new JSONObject(Objects.requireNonNull(response.body()).string());
          JSONArray jsonArray = json.getJSONArray("Results");
          for (int i = 0; i < jsonArray.length(); i++) {
            HashMap<String, ByteIterator> resultMap = new HashMap<>();
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
    batchRecord.add(batchUpdateBuilder(values, key));
    records += batchRecord + "\n]\n}";

    RequestBody insertBody = RequestBody.create(records, MEDIA_TYPE);
    Request request = requestBuilder(insertBody, httpMethod, path);
    try (Response response = client.newCall(request).execute()) {
      if (response.isSuccessful()) {
        if (concurrency) {
          JSONObject json = new JSONObject(Objects.requireNonNull(response.body()).string());
          JSONArray jsonArray = json.getJSONArray("Results");
          writeChangeVectorUpdate(key, jsonArray);
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

  private static String[] REPLACE_INPUT = new String[] { "\"", "\\" };
  private static String[] REPLACE_OUTPUT = new String[] { "\\\"", "\\\\" };

  private String insertBuilder(Map<String, ByteIterator> values) {
    StringBuilder records = new StringBuilder(2048);
    records.append("{");

    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      String value = entry.getValue().toString();
      // Escape backslash and quotation marks to get valid json
      value = StringUtils.replaceEach(value, REPLACE_INPUT, REPLACE_OUTPUT);
      records.append("\"").append(entry.getKey()).append("\": \"").append(value).append("\",");
    }

    records.append("\"@metadata\":{\"@collection\":\"records\"}}");
    return records.toString();
  }

  private String batchedInsertBuilder(Map<String, ByteIterator> values, String key) {
    return "{\"Id\":\"" + key + "\",\"ChangeVector\":null,\"Document\":" +
        insertBuilder(values) +
        "\"Type\": \"PUT\"}";
  }

  private String batchUpdateBuilder(Map<String, ByteIterator> values, String key) {
    if (concurrency && changeVectors.get(key) != null) {
      return " {\n" + "   \"Id\":\"" + key + "\",\n" +
          "   \"ChangeVector\": \"" + changeVectors.get(key) + "\",\n" +
          "   \"Patch\":" +
          updateBuilder(values) +
          "   \"Type\": \"PATCH\"\n" +
          "}";
    } else {
      return " {\n" + "   \"Id\":\"" + key + "\",\n" +
          "   \"ChangeVector\": null,\n" +
          "   \"Patch\":" +
          updateBuilder(values) +
          "   \"Type\": \"PATCH\"\n" +
          "}";
    }
  }

  private String updateBuilder(Map<String, ByteIterator> values) {
    StringBuilder records = new StringBuilder(" {\n \"Script\":\"");

    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      String value = entry.getValue().toString();
      // Replace backslash, quotation marks and double quotation marks to get valid json
      value = value.replaceAll("\\\\", "a");
      value = value.replaceAll("\"", "b");
      value = value.replaceAll("'", "c");
      records.append("this.").append(entry.getKey()).append("='").append(value).append("';");
    }

    records.append("\",\n},");
    return records.toString();
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

  protected void writeChangeVectorUpdate(String id, JSONArray jsonArray) {
    for (Object o : jsonArray) {
      if (!JSONObject.NULL.equals(o)) {
        JSONObject jsonLineItem = (JSONObject) o;
        changeVectors.put(id, jsonLineItem.get("ChangeVector").toString());
      }
    }
  }

  protected void writeChangeVectorWrite(JSONArray jsonArray) {
    for (Object o : jsonArray) {
      if (!JSONObject.NULL.equals(o)) {
        JSONObject jsonLineItem = (JSONObject) o;
        changeVectors.put(jsonLineItem.get("@id").toString(), jsonLineItem.get("@change-vector").toString());
      }
    }
  }
}
