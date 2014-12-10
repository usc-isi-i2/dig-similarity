package edu.isi.dig.elasticsearch;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.MoreLikeThisQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticSearchHandler {
	
	/*final static String SEARCH_RESULTS="results";
	final static String CLUSTER_NAME = "cluster.name";
	final static String CLUSTER_NAME_VALUE = "dig_isi";
	final static String ELASTICSEARCH_HOST = "localhost"; 
	final static int ELASTICSEARCH_PORT = 9300;*/
	final static String BODY_PART="hasBodyPart";
	final static String TEXT = "text";
	final static String fileName = "config.properties";

	
	static Client esClient=null;
	static TransportClient ts =null;
	static SearchResponse searchResp = null;
	static Properties prop=null;
	static String indexName="";
	static String docType="";
	
			
	static Settings settings = null;
	
	private static Logger LOG = LoggerFactory.getLogger(ElasticSearchHandler.class);
	
	public static void Initialize(){
		
		
		prop = new Properties();
		InputStream input = ElasticSearchHandler.class.getClassLoader().getResourceAsStream(fileName);
		try{
			prop.load(input);
			settings = ImmutableSettings.settingsBuilder()
					.put(prop.getProperty("clusterNameProperty"), prop.getProperty("clusterName")).build();

			ts = new TransportClient(settings);

			esClient = ts.addTransportAddress(new InetSocketTransportAddress(prop.getProperty("elasticsearchHost"), 
																			Integer.parseInt(prop.getProperty("elasticsearchPort"))));
			
			indexName = prop.getProperty("indexName");
			docType = prop.getProperty("docType");
			
		}catch(IOException ioe){
			ioe.printStackTrace();
		}
		
		
	}
	
	
	public static String PerformSimpleSearch(String uri){
		
		try{
			Initialize();
			
			TermQueryBuilder termQB = QueryBuilders.termQuery("uri", uri);
				
				
			SearchResponse searchResp = esClient.prepareSearch(indexName)
												.setTypes(docType)
												.setQuery(termQB)
												.execute()
												.actionGet();
				
			SearchHit[] searchHit = searchResp.getHits().getHits();
			
			
			if(searchHit.length == 1){
				
				return searchHit[0].getSourceAsString();
			}
			
		return null;
		
		}catch(Exception e){
			return e.toString();
		}
		finally{
			
			if(ts!=null)
				ts.close();
			
			if(esClient!=null)
				esClient.close();
			
		}
	}
	
	
	public static String FindSimilar(String uri,String sendBack){
		
		try{
			String searchSourceJson = PerformSimpleSearch(uri);
			
			if(searchSourceJson!=null){
				
				JSONObject jSourceObj = (JSONObject) JSONSerializer.toJSON(searchSourceJson);
				
				if(jSourceObj.containsKey(BODY_PART)){
					
					JSONObject jBodyPart = (JSONObject) jSourceObj.get("hasBodyPart");
					
					if(jBodyPart.containsKey(TEXT)){
						
						String bodyText = jBodyPart.getString("text");
						
						//LOG.debug(bodyText);
						MoreLikeThisQueryBuilder qb = QueryBuilders.moreLikeThisQuery("hasBodyPart.text.shingle_4")
																   .likeText(bodyText)
																   .minTermFreq(1)
																   .maxQueryTerms(20);
						
						
						
						Initialize();
						searchResp = esClient.prepareSearch(indexName)
											 .setTypes(docType)
											 .setQuery(qb)
											 .execute()
											 .actionGet();
						
						SearchHit[] searchHit = searchResp.getHits().getHits();
						JSONObject jParentObj= new JSONObject();
						JSONArray jArray = new JSONArray();
						
						for(SearchHit sh : searchHit){
							
							JSONObject jTemp = (JSONObject)JSONSerializer.toJSON(sh.getSourceAsString());
							String resultURI = jTemp.getString("uri");
							LOG.debug("sendBAck:" + sendBack);
							if(sendBack.equalsIgnoreCase("all")){
								jArray.add(jTemp);
							}
							else{
								jArray.add(resultURI);
							}
						}
						
						jParentObj.accumulate("similar", jArray);
						
						return jParentObj.toString();
					}
					else{
						throw new Exception("hasBodyPart for URI: " + uri + " doesnot contain field 'text'");
					}
					
				}
				else{
					throw new Exception("Json Object  for URI: " + uri + " doesnot contain 'hasBodyPart'");
				}
				
			}
			else{
				throw new Exception("No WebPage found for uri: " + uri);
			}
		
		}catch(Exception e){
			return e.toString();
		}
		finally{
			
			if(ts!=null)
				ts.close();
			
			if(esClient!=null)
				esClient.close();
			
		}
	}

}
