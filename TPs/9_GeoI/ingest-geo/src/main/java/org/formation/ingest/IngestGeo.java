package org.formation.ingest;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.regex.PatternSyntaxException;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

public class IngestGeo {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub

		if (args.length < 2) {
			System.out.println("Usage java -jar ingest.jar <dir> <index> [<http_host> <http_port>]");
			System.exit(0);
		}
		String host = "localhost";
		int port = 9200;
		if (args.length >= 3) {
			host = args[2];
		}
		if (args.length == 4) {
			port = Integer.parseInt(args[3]);
		}
		RestClient restClient = RestClient.builder(new HttpHost(host, port, "http")).build();

		Path dir = Paths.get(args[0]);
		String index = args[1];
		
		BufferedReader reader = null;
		try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "*.*")) {
			for (Path file : stream) {
				System.out.println("File :" + file);
				reader = new BufferedReader(new FileReader(file.toFile()));
				String sCurrentLine;
				while ((sCurrentLine = reader.readLine()) != null) {
					sCurrentLine = StringEscapeUtils.escapeJava(sCurrentLine);
					HttpEntity entity = new NStringEntity("{\n" + "    \"message\" : \"" + sCurrentLine + "\"\n" + "}",
							ContentType.APPLICATION_JSON);
					try {
						Request postRequest = new Request("POST", "/" + index + "/_doc/?pipeline=access_log");
						postRequest.setEntity(entity);
						Response indexResponse = restClient.performRequest(postRequest);

						System.out.println(EntityUtils.toString(indexResponse.getEntity()));
					} catch (PatternSyntaxException | DirectoryIteratorException | IOException e) {
						System.err.println(e);
					}
				}
				System.out.println(file + ":indexed");
			}
		} catch (IOException x) {
			System.err.println(x);
		} finally {
			if (reader != null) {
				reader.close();
			}
		}

		restClient.close();
	}

}
