package edu.isi.dig.service;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.isi.dig.elasticsearch.ElasticSearchHandler;

@Path("/")
public class SimilarityService {
	
	private static Logger LOGS = LoggerFactory.getLogger(SimilarityService.class);
	
	@GET
	@Path("/similar/ads")
	public String FindSimilarAds(@QueryParam("uri") String uri,@QueryParam("return") String sendBack) throws Exception{
		
		if(uri!=null){
			if(sendBack!=null && (sendBack.equalsIgnoreCase("uri") || sendBack.equalsIgnoreCase("all"))){
				
				return ElasticSearchHandler.FindSimilar(uri,sendBack);
			}
			
			return ElasticSearchHandler.FindSimilar(uri,"all");
		}
		
		throw new Exception("Required parameter 'uri' is null");

		
	}
	
	
	@GET
	@Path("/similar/images")
	public String FindImagesAndUpdateES(@QueryParam("uri") String uri) throws Exception{
		
		if(uri != null){
			
			CloseableHttpClient httpClient = HttpClients.createDefault();
			HttpGet httpGet = new HttpGet("http://sentibank.zapto.org/getSimilar.php?url="+uri+"&fast=1");
			
			CloseableHttpResponse httpResponse = httpClient.execute(httpGet);
			
			JSONObject jResults = new JSONObject();
			LOGS.debug("ResponseCode:"+ httpResponse.getStatusLine().getStatusCode());
			if(httpResponse != null && httpResponse.getStatusLine().getStatusCode() >=200 && httpResponse.getStatusLine().getStatusCode() < 300){
				
				try{
					HttpEntity httpEntity = httpResponse.getEntity();
					
					String jsonResponse = EntityUtils.toString(httpEntity);

					if(jsonResponse!=null && !jsonResponse.trim().equals("")){
						
						JSONObject jImages = (JSONObject)JSONSerializer.toJSON(jsonResponse);
						
						if(jImages.containsKey("images")){
							Object jSimImages = jImages.get("images");
							
							if(jSimImages instanceof JSONObject){
								JSONObject jObjSimImages = (JSONObject) jSimImages;
								
								if(jObjSimImages.containsKey("similar_images")){
									JSONObject jSimilarImages = jObjSimImages.getJSONObject("similar_images");
									if(jSimilarImages.containsKey("cached_image_urls")){
										Object jCachedImageUrls = jSimilarImages.get("cached_image_urls");
										if(jCachedImageUrls instanceof JSONArray){
											jResults = ElasticSearchHandler.UpdateWebPagesWithSimilarImages((JSONArray) jCachedImageUrls,uri);
											
										}
										//TODO check if it is a JSONObject. Shouldn't be though
									}
								}
								//TODO: check for 'image_urls' as well. This is null as of now as returned by Tao's service
								
							}
							else if(jSimImages instanceof JSONArray){
								
								JSONArray jArrayImages = (JSONArray) jSimImages;
								
								for(int i =0; i<jArrayImages.size();i++){
									
									JSONObject jObjSimImages = (JSONObject) jArrayImages.get(i);
									
									if(jObjSimImages.containsKey("similar_images")){
										JSONObject jSimilarImages = jObjSimImages.getJSONObject("similar_images");
										
										if(jSimilarImages.containsKey("cached_image_urls")){
											Object jCachedImageUrls = jSimilarImages.get("cached_image_urls");
											if(jCachedImageUrls instanceof JSONArray){
												jResults = ElasticSearchHandler.UpdateWebPagesWithSimilarImages((JSONArray) jCachedImageUrls,uri);
												
											}
											//TODO check if it is a JSONObject. Shouldn't be though
										}
									}
									
								}
							}
							
						}
						httpClient.close();
						
					}	
				}
				catch(Exception e){
					throw e;
				}
			}
			
			return jResults.toString();

			
			
		}else{
			
			throw new Exception("Required parameter 'uri' is null");
		}
		
	}

}
