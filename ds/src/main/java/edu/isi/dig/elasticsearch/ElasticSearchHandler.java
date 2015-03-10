package edu.isi.dig.elasticsearch;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.MoreLikeThisQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;

import edu.isi.dig.service.SimilarityService.ImageRank;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;


public class ElasticSearchHandler {
	
	final static String BODY_PART="hasBodyPart";
	final static String TEXT = "text";
	final static String fileName = "config.properties";
	final static String IMAGE_CACHE_URL = "hasImagePart.cacheUrl";
	final static String SIMILAR_IMAGES = "similar_images_feature";
	final static String FEATURE_VALUE_LABEL = "featureValue";
	final static String FEATURE_NAME_LABEL = "featureName";
	final static String FEATURE_NAME = "similarimageurl";
	final static String HAS_FEATURE_COLLECTION = "hasFeatureCollection";
	final static String HAS_IMAGE_PART = "hasImagePart";
	final static String URI = "uri";
	final static String FEATURE_OBJECT = "featureObject";
	final static String IMAGE_OBJECT_URI_RANKS="imageObjectUriRanks";
	final static String IMAGE_OBJECT_URIS = "imageObjectUris";
	final static String IMAGE_OBJECT_URI = "imageObjectUri";
	final static String IMAGE_RANK = "imageRank";
	final static String CACHE_URL = "cacheUrl";

	
	static Client esClient=null;
	static TransportClient ts =null;
	static SearchResponse searchResp = null;
	static Properties prop=null;
	static String indexName="";
	static String docType="";
	static String environment="";
	static String returnPort = "9200";
	static String elasticsearchHost="";
	
			
	static Settings settings = null;
	
	//private static Logger LOG = LoggerFactory.getLogger(ElasticSearchHandler.class);
	
	public static void Initialize(){
		
		
		prop = new Properties();
		InputStream input = ElasticSearchHandler.class.getClassLoader().getResourceAsStream(fileName);
		try{
			prop.load(input);
			
			elasticsearchHost=prop.getProperty("elasticsearchHost");
			
			settings = ImmutableSettings.settingsBuilder()
					.put(prop.getProperty("clusterNameProperty"), prop.getProperty("clusterName")).build();
			ts = new TransportClient(settings);

			esClient = ts.addTransportAddress(new InetSocketTransportAddress(elasticsearchHost, 
																			Integer.parseInt(prop.getProperty("elasticsearchPort"))));
			
			indexName = prop.getProperty("indexName");
			docType = prop.getProperty("docType");
			environment = prop.getProperty("environment");
		
			
			if(environment.equals("production")){
				returnPort = prop.getProperty("nginxPort");
			}
			
		}catch(IOException ioe){
			ioe.printStackTrace();
		}
		
		
	}
	
	
	public static String PerformSimpleSearch(String uri,String differentIndex){
		
		try{
			Initialize();
			
			String indexToUse = null;
			
			if(differentIndex != null){
				
				indexToUse = differentIndex;
			}else {
				
				indexToUse = indexName;
			}
			TermQueryBuilder termQB = QueryBuilders.termQuery(URI, uri);
				
				
			SearchResponse searchResp = esClient.prepareSearch(indexToUse)
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
		
		if(jSource.containsKey("hasFeatureCollection")){
			
			JSONObject jFeatureCollection = jSource.getJSONObject("hasFeatureCollection");
			
			@SuppressWarnings("unchecked")
			Set<String> keys = jFeatureCollection.keySet();
			
			
			
			for(String key : keys){
				
				Object jFeature = jFeatureCollection.get(key);
				
				if(jFeature instanceof JSONObject){
					if(((JSONObject) jFeature).containsKey("featureName") && ((JSONObject) jFeature).containsKey("featureValue")){
						mapFeatureCollection.put(((JSONObject) jFeature).getString("featureName"), ((JSONObject) jFeature).getString("featureValue"));
					}
				}else if(jFeature instanceof JSONArray){
					
					JSONArray JFeatures = (JSONArray) jFeature;
					//if its an array, it has multiple values for the feature, 
					ArrayList<String> multipleValues = new ArrayList<String>();
					
					if(JFeatures.size() > 0){
						for(int i=0; i < JFeatures.size();i++){
							
							JSONObject jTemp = JFeatures.getJSONObject(i);
							
							if(((JSONObject) jTemp).containsKey("featureName") && ((JSONObject) jTemp).containsKey("featureValue")){
						
								multipleValues.add(((JSONObject) jTemp).getString("featureValue"));
							}
						}
						//get the featureName,multipleValues map
						mapFeatureCollection.put(JFeatures.getJSONObject(0).getString("featureName"), multipleValues);
					}
				}
			}
		}
		return mapFeatureCollection;
	}
	
	public static String FindSimilar(String uri,String sendBack,String differentIndex){
		
		try{
			String searchSourceJson = PerformSimpleSearch(uri,differentIndex);
			
			Map<String,Object> mapSourceFeatures = new HashMap<String, Object>();
			
			if(searchSourceJson!=null){
				
				JSONObject jSourceObj = (JSONObject) JSONSerializer.toJSON(searchSourceJson);
				
				if(jSourceObj.containsKey(BODY_PART)){
					
					JSONObject jBodyPart = (JSONObject) jSourceObj.get(BODY_PART);
					
					if(jBodyPart.containsKey(TEXT)){
						
						String bodyText = jBodyPart.getString(TEXT);
						
						//Create a map of <featureValues,featureNames>
						mapSourceFeatures = collectFeatures(searchSourceJson);
						
						//ToDo: Make this query better
						MoreLikeThisQueryBuilder qb = QueryBuilders.moreLikeThisQuery("hasBodyPart.text.shingle_4")
																   .likeText(bodyText)
																   .minTermFreq(1)
																   .maxQueryTerms(20);
						
						
						
						Initialize();
						
						String indexToUse = null;
						
						if(differentIndex != null){
							
							indexToUse = differentIndex;
						}else {
							
							indexToUse = indexName;
						}
						
						searchResp = esClient.prepareSearch(indexToUse)
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
							
							JSONObject similarWebPageResult = new JSONObject();
							
							
							JSONObject jSimilarWebPage = (JSONObject)JSONSerializer.toJSON(sh.getSourceAsString());
							
							Map<String,Object> mapSimilarWPFC = collectFeatures(sh.getSourceAsString());
							
							Set<String> similarWebPagekeys =mapSimilarWPFC.keySet();
							
							for(String key : similarWebPagekeys){
								
								if(mapSourceFeatures.containsKey(key)){
									
									Object sourceFeatureValue = mapSourceFeatures.get(key);
									Object targetFeatureValue = mapSimilarWPFC.get(key);
									
									if(sourceFeatureValue instanceof String && targetFeatureValue instanceof String){
										
										if(!(String.valueOf(sourceFeatureValue).equals(String.valueOf(targetFeatureValue)))){
											JSONObject jTemp = new JSONObject();
											jTemp.accumulate(key, String.valueOf(targetFeatureValue));
											differentValuedFeatures.add(jTemp.toString());
										}
									}
									else if(sourceFeatureValue instanceof ArrayList<?> && targetFeatureValue instanceof ArrayList<?>){
										
										for(int i=0;i < ((ArrayList<?>) targetFeatureValue).size();i++){
											
											if(!((ArrayList<?>) sourceFeatureValue).contains(((ArrayList<?>) targetFeatureValue).get(i))){
												
												JSONObject jTemp = new JSONObject();
												jTemp.accumulate(key, ((ArrayList<?>) targetFeatureValue).get(i));
												differentValuedFeatures.add(jTemp.toString());
											}
										}
									}
									else{//one is string and other is ArrayList
										
										if(sourceFeatureValue instanceof String){//source string, target ArrayList
											
											for(int i=0;i < ((ArrayList<?>) targetFeatureValue).size();i++){
												
												if(!((ArrayList<?>) targetFeatureValue).get(i).equals(String.valueOf(sourceFeatureValue))){
													
													JSONObject jTemp = new JSONObject();
													jTemp.accumulate(key, ((ArrayList<?>) targetFeatureValue).get(i));
													differentValuedFeatures.add(jTemp.toString());
												}
											}
											
										}else{//Source - ArrayList , target - String
											
											if(!((ArrayList<?>) sourceFeatureValue).contains(String.valueOf(targetFeatureValue))){
												
												JSONObject jTemp = new JSONObject();
												jTemp.accumulate(key, String.valueOf(targetFeatureValue));
												differentValuedFeatures.add(jTemp.toString());
											}
											
										}
										
									}
									
				
								}
								else{
									//additional features, not present in the source json but in similar json objects
									JSONObject jTemp = new JSONObject();
									
									Object value = mapSimilarWPFC.get(key);
									
									if(value instanceof ArrayList<?>){
										
										for(int i=0;i< ((ArrayList<?>) value).size();i++){
											jTemp.accumulate(key, ((ArrayList<?>) value).get(i));
										}
									}
									else{
										jTemp.accumulate(key, value);
										
									}
									
									additionalFeatures.add(jTemp.toString());
								}
							}
							
							Set<String> sourceKeys = mapSourceFeatures.keySet();
							
							for(String key: sourceKeys){
								if(!mapSimilarWPFC.containsKey(key)){
									JSONObject jTemp = new JSONObject();
									
									Object value = mapSourceFeatures.get(key);
									
									if(value instanceof ArrayList<?>){
										
										for(int i=0;i< ((ArrayList<?>) value).size();i++){
											jTemp.accumulate(key, ((ArrayList<?>) value).get(i));
										}
									}
									else{
										jTemp.accumulate(key, value);
										
									}
									
									missingFeatures.add(jTemp.toString());
								}
							}
							
							String resultURI = jSimilarWebPage.getString(URI);
							
							if(sendBack.equalsIgnoreCase("all")){
								similarWebPageResult.accumulate("similarWebPage",jSimilarWebPage);
							}
							else{
								JSONObject jSimilarBodyPart = jSimilarWebPage.getJSONObject(BODY_PART);
								similarWebPageResult.accumulate(URI,resultURI);
								similarWebPageResult.accumulate(BODY_PART + "."+ TEXT, jSimilarBodyPart.getString(TEXT));
							}
							
							
							if(additionalFeatures.size() > 0){
								similarWebPageResult.accumulate("additionalFeatures", additionalFeatures);
							}
							
							if(differentValuedFeatures.size()>0){
								similarWebPageResult.accumulate("featuresWithDifferentValues",differentValuedFeatures);
							}
							if(missingFeatures.size() > 0){
								similarWebPageResult.accumulate("missingFeatures",missingFeatures);
							}
							
							jArray.add(new JSONObject().accumulate("similarWebPage",similarWebPageResult));
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

	
	public static JSONObject addSimilarImagesFeature(JSONObject source, String queryURI,ImageRank matchedImage) {
		
		
		if(source.containsKey(HAS_FEATURE_COLLECTION)){
			
			JSONObject jHFC = source.getJSONObject(HAS_FEATURE_COLLECTION);//Assumption: it is a json object. I would be surprised if it isnt
			
			boolean containsFeatureObject = false;
			JSONObject jObjFeatureObject = new JSONObject();
			
			if(jHFC.containsKey(SIMILAR_IMAGES)){
				
				JSONArray jSimImages = jHFC.getJSONArray(SIMILAR_IMAGES);
				
				
				boolean containsURI = false;
				for(int i=0;i<jSimImages.size();i++){
					
					if(jSimImages.getJSONObject(i).containsKey(FEATURE_OBJECT)){
						
						containsFeatureObject = true;
						jObjFeatureObject = jSimImages.getJSONObject(i).getJSONObject(FEATURE_OBJECT);
						
						if(jObjFeatureObject.containsKey(IMAGE_OBJECT_URIS)){
							jSimImages.remove(i); //historic data, should contain IMAGE_OBJECT_URI_RANKS
							containsFeatureObject =false;
						}
						
					}
					else {
						
							JSONObject jSimImage = jSimImages.getJSONObject(i);
						if(jSimImage.getString(FEATURE_VALUE_LABEL).equals(queryURI)){
							
							containsURI = true;
						}
					}
				
					
				}
				if(!containsURI){
				
					jSimImages.add(accumulateSimilarImageFeature(queryURI));
				}
				
				if(containsFeatureObject){ //check if the uri being added is not already in there

						
						JSONArray jArrayImageURIs = jObjFeatureObject.getJSONArray(IMAGE_OBJECT_URI_RANKS);
						
						//remove everything 
						for(int i=0;i<jArrayImageURIs.size();i++){
							jArrayImageURIs.remove(i);
						}
						//LOG.info("SIZE NOW:"+jArrayImageURIs.size());
						Object ObjImagePart = source.get(HAS_IMAGE_PART);
						
						if(ObjImagePart instanceof JSONArray){
						
							JSONArray jArrayImagePart = (JSONArray) ObjImagePart;
							
							for(int j=0;j<jArrayImagePart.size();j++){
									
								JSONObject jObjImage = jArrayImagePart.getJSONObject(j);
								
								if(jObjImage.containsKey(CACHE_URL)){
									
									if(jObjImage.getString(CACHE_URL).equals(matchedImage.getImageURL())){
										
										jArrayImageURIs.add(addImageRank(jObjImage.getString(URI), matchedImage.getRank()));
									}
								}
							}
						}
						else if(ObjImagePart instanceof JSONObject){
							
							JSONObject jObjImage = (JSONObject) ObjImagePart;
							
							if(jObjImage.containsKey(CACHE_URL)){
								
								if(jObjImage.getString(CACHE_URL).equals(matchedImage.getImageURL())){
										
									jArrayImageURIs.add(addImageRank(jObjImage.getString(URI), matchedImage.getRank()));
								}
							}
						}
					
				}else { //add 'featureObject'
					
					jSimImages.add(addFeatureObject(source,matchedImage));
					
				}
				
			}
			else {
				
				JSONArray jNewSimImages = new JSONArray();
				jNewSimImages.add(accumulateSimilarImageFeature(queryURI));
				
				jNewSimImages.add(addFeatureObject(source,matchedImage));
				jHFC.accumulate(SIMILAR_IMAGES, jNewSimImages);
				
			}
			
		}
		//LOG.info(source.toString());
		return source;
	}
	
	
	public static JSONObject addImageRank(String imageURI, double rank){
		
		JSONObject jImageRank = new JSONObject();
		
		jImageRank.accumulate(IMAGE_OBJECT_URI, imageURI);
		jImageRank.accumulate(IMAGE_RANK, rank);
		return jImageRank;
	}
	
	
	public static JSONObject addFeatureObject(JSONObject source,ImageRank matchedImage){
		
		Object objImagePart = source.get(HAS_IMAGE_PART);
		
		JSONObject jObjReturn = new JSONObject();
		
		if(objImagePart instanceof JSONArray) {
			
			//JSONArray jArrayImagePart = source.getJSONArray(HAS_IMAGE_PART);//guaranteed to be in there
			JSONArray jArrayImagePart = (JSONArray) objImagePart;//guaranteed to be in there
			
			for(int i=0;i<jArrayImagePart.size();i++){
				
				JSONObject jObjImage = jArrayImagePart.getJSONObject(i);
				
				if(jObjImage.containsKey(CACHE_URL)){
					
					if(jObjImage.getString(CACHE_URL).equals(matchedImage.getImageURL())){
						
						JSONArray jArrayImageURIs = new JSONArray();
						jArrayImageURIs.add(addImageRank(jObjImage.getString(URI), matchedImage.getRank()));
						
						JSONObject jObjFeatureObject = new JSONObject();
						jObjFeatureObject.accumulate(IMAGE_OBJECT_URI_RANKS, jArrayImageURIs);
						
						jObjReturn.accumulate(FEATURE_OBJECT, jObjFeatureObject);
						break;
						
					}
				}
			}
		}
		else if(objImagePart instanceof JSONObject) {
			
			JSONObject jObjImagePart = (JSONObject) objImagePart;
			
			if(jObjImagePart.containsKey(CACHE_URL)){
				
				if(jObjImagePart.getString(CACHE_URL).equals(matchedImage.getImageURL())){
					
					JSONArray jArrayImageURIs = new JSONArray();
					jArrayImageURIs.add(addImageRank(jObjImagePart.getString(URI), matchedImage.getRank()));
					
					JSONObject jObjFeatureObject = new JSONObject();
					jObjFeatureObject.accumulate(IMAGE_OBJECT_URI_RANKS, jArrayImageURIs);
					
					jObjReturn.accumulate(FEATURE_OBJECT, jObjFeatureObject);
					
				}
			}
			
		}
		return jObjReturn;
	}
	public static JSONObject accumulateSimilarImageFeature(String queryURI){
		
		JSONObject jNewSimImage = new JSONObject();
		jNewSimImage.accumulate(FEATURE_NAME_LABEL, FEATURE_NAME);
		jNewSimImage.accumulate(FEATURE_NAME, queryURI);
		jNewSimImage.accumulate(FEATURE_VALUE_LABEL, queryURI);
		
		return jNewSimImage;
		
	}
	
	
	public static JSONObject UpdateWebPagesWithSimilarImages(ArrayList<ImageRank> jArray,String queryURI,String differentIndex) throws Exception{
		try{
			Initialize();
			JSONObject jResults = new JSONObject();
			
			String indexToUse = null;
			
			if(differentIndex != null){
				
				indexToUse = differentIndex;
			}else {
				
				indexToUse = indexName;
			}
			
			System.out.println("start time:" + new Date().toString());
			
			
			BulkRequestBuilder bulkRB = new BulkRequestBuilder(esClient);
			
			
			for(int i=0;i<jArray.size();i++){
				
				
				TermQueryBuilder termQB = QueryBuilders.termQuery(IMAGE_CACHE_URL, jArray.get(i).getImageURL());
					
					
				SearchResponse searchResp = esClient.prepareSearch(indexToUse)
													.setTypes(docType)
													.setQuery(termQB)
													.execute()
													.actionGet();
					
				SearchHit[] searchHit = searchResp.getHits().getHits();
				
				for(SearchHit hit : searchHit){
					
//					LOG.debug("Ads id: "+ hit.getId());
					
					String docId = hit.getId();
					
					JSONObject jUpdatedSource = addSimilarImagesFeature((JSONObject) JSONSerializer.toJSON(hit.getSourceAsString()),
							                                            queryURI,
							                                            jArray.get(i));
					
					UpdateRequest updateRequest = new UpdateRequest();
					updateRequest.index(indexToUse);
					updateRequest.type(docType);
					updateRequest.id(docId);
					updateRequest.doc(jUpdatedSource);
					
					bulkRB.add(updateRequest);
					//esClient.update(updateRequest).get();
					
					
					jResults.accumulate("ad_uri", "http://" + 
												   elasticsearchHost + ":" + 
												   returnPort + "/" +
												   indexToUse + "/" + 
												   docType + "/" + 
												   docId);
				}
				
			}
			
			bulkRB.execute().actionGet();
			System.out.println("end time:" + new Date().toString());
			return jResults;
		
		}catch(Exception e){
			throw e;
		}
		finally{
			
			if(ts!=null)
				ts.close();
			
			if(esClient!=null)
				esClient.close();
			
		}
	}
}
