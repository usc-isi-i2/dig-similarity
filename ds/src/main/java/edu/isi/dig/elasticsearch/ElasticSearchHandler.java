package edu.isi.dig.elasticsearch;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

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
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

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
	
	//private static Logger LOG = LoggerFactory.getLogger(ElasticSearchHandler.class);
	
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
	
	
	private static Map<String,Object> collectFeatures(String jsonWebPage){
		
		//AS per our current standards, should be a JSON object
		JSONObject jSource = (JSONObject) JSONSerializer.toJSON(jsonWebPage);
		Map<String, Object> mapFeatureCollection = new HashMap<String,Object>();
		
		//LOG.debug(jsonWebPage);
		if(jSource.containsKey("hasFeatureCollection")){
			
			JSONObject jFeatureCollection = jSource.getJSONObject("hasFeatureCollection");
			
			@SuppressWarnings("unchecked")
			Set<String> keys = jFeatureCollection.keySet();
			
			
			
			for(String key : keys){
				
				Object jFeature = jFeatureCollection.get(key);
				
				if(jFeature instanceof JSONObject){
					if(((JSONObject) jFeature).containsKey("featureName") && ((JSONObject) jFeature).containsKey("featureValue")){
						mapFeatureCollection.put(((JSONObject) jFeature).getString("featureName"), ((JSONObject) jFeature).get("featureValue"));
					}
				}else if(jFeature instanceof JSONArray){
					
					JSONArray JFeatures = (JSONArray) jFeature;
					
					for(int i=0; i < JFeatures.size();i++){
						
						JSONObject jTemp = JFeatures.getJSONObject(i);
						
						if(((JSONObject) jTemp).containsKey("featureName") && ((JSONObject) jTemp).containsKey("featureValue")){
							mapFeatureCollection.put(((JSONObject) jTemp).getString("featureName"), ((JSONObject) jTemp).get("featureValue"));
						}
					}
				}
			}
		}
		return mapFeatureCollection;
	}
	
	public static String FindSimilar(String uri,String sendBack){
		
		try{
			String searchSourceJson = PerformSimpleSearch(uri);
			
			Map<String,Object> mapSourceFeatures = new HashMap<String, Object>();
			
			if(searchSourceJson!=null){
				
				JSONObject jSourceObj = (JSONObject) JSONSerializer.toJSON(searchSourceJson);
				
				if(jSourceObj.containsKey(BODY_PART)){
					
					JSONObject jBodyPart = (JSONObject) jSourceObj.get("hasBodyPart");
					
					if(jBodyPart.containsKey(TEXT)){
						
						String bodyText = jBodyPart.getString("text");
						
						//Create a map of <featureValues,featureNames>
						mapSourceFeatures = collectFeatures(searchSourceJson);
						
						
						/*Set<String> keys = mapSourceFeatures.keySet();
						for(String key : keys){
							LOG.debug(key + ":" + mapSourceFeatures.get(key));
						}*/
						
						
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
							
							JSONArray additionalFeatures = new JSONArray();
							JSONArray missingFeatures = new JSONArray();
							JSONArray differentValuedFeatures = new JSONArray();
							
							
							JSONObject jSimilarWebPage = (JSONObject)JSONSerializer.toJSON(sh.getSourceAsString());
							
							Map<String,Object> mapSimilarWPFC = collectFeatures(sh.getSourceAsString());
							
							Set<String> similarWebPagekeys =mapSimilarWPFC.keySet();
							
							for(String key : similarWebPagekeys){
								
								if(mapSourceFeatures.containsKey(key)){
									
									String sourceFeatureValue = String.valueOf(mapSourceFeatures.get(key));
									String targetFeatureValue = String.valueOf(mapSimilarWPFC.get(key));
									
									//LOG.debug("Source:"+sourceFeatureValue);
									//LOG.debug("Target:"+targetFeatureValue);
									
									if(!(sourceFeatureValue.equals(targetFeatureValue))){
											JSONObject jTemp = new JSONObject();
											jTemp.accumulate(key, mapSimilarWPFC.get(key));
											differentValuedFeatures.add(jTemp);
											
											
									}
									
								}
								else{
									//additional features
									JSONObject jTemp = new JSONObject();
									
									if(key.equals("person_age")){//will regret harcoding it here
										jTemp.accumulate(key, Integer.parseInt(String.valueOf(mapSimilarWPFC.get(key))));
									}else{
										jTemp.accumulate(key, mapSimilarWPFC.get(key));
									}
									
									
									additionalFeatures.add(jTemp);
								}
							}
							
							Set<String> sourceKeys = mapSourceFeatures.keySet();
							
							for(String key: sourceKeys){
								if(!mapSimilarWPFC.containsKey(key)){
									JSONObject jTemp = new JSONObject();
									jTemp.accumulate(key, mapSourceFeatures.get(key));
									missingFeatures.add(jTemp);
								}
							}
							
							String resultURI = jSimilarWebPage.getString("uri");
							
							if(sendBack.equalsIgnoreCase("all")){
								jArray.add(new JSONObject().accumulate("WebPage",jSimilarWebPage));
							}
							else{
								jArray.add(new JSONObject().accumulate("WebPage", resultURI));
							}
							
							
							if(additionalFeatures.size() > 0){
								jArray.add(new JSONObject().accumulate("additionalFeatures", additionalFeatures));
							}
							
							if(differentValuedFeatures.size()>0){
								jArray.add(new JSONObject().accumulate("FeaturesWithDifferentValues",differentValuedFeatures));
							}
							if(missingFeatures.size() > 0){
								jArray.add(new JSONObject().accumulate("missingFeatures",missingFeatures));
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
