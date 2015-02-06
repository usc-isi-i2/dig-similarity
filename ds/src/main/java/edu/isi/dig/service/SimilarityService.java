package edu.isi.dig.service;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;

import org.apache.http.HttpEntity;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLContextBuilder;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

import edu.isi.dig.elasticsearch.ElasticSearchHandler;

@Path("/")
public class SimilarityService {
	
	//private static Logger LOGS = LoggerFactory.getLogger(SimilarityService.class);
	static Properties prop=null;
	static String imageSimilarityHost=null;
	static String imageSimilarityPort=null;
	static String imageSimilarityUserName=null;
	static String imageSimilarityPassword=null;
	static String imageSimilarityProtocol=null;
	final static String fileName = "config.properties";
	
public static void Initialize(){
		
		
		prop = new Properties();
		InputStream input = SimilarityService.class.getClassLoader().getResourceAsStream(fileName);
		try{
			prop.load(input);
			
			imageSimilarityHost=prop.getProperty("imageSimilarityhost");
			imageSimilarityPort=prop.getProperty("imageSimilarityPort");
			imageSimilarityUserName=prop.getProperty("imageSimilarityUserName");
			imageSimilarityPassword=prop.getProperty("imageSimilarityPassword");
			imageSimilarityProtocol = prop.getProperty("imageSimilarityProtocol");
			
			
		}catch(IOException ioe){
			ioe.printStackTrace();
		}
		
		
	}
	
	@GET
	@Path("/similar/ads")
	public String FindSimilarAds(@QueryParam("uri") String uri,@QueryParam("return") String sendBack,@QueryParam("index") String indexName) throws Exception{
		
		if(uri!=null){
			if(sendBack!=null && (sendBack.equalsIgnoreCase("uri") || sendBack.equalsIgnoreCase("all"))){
				
				return ElasticSearchHandler.FindSimilar(uri,sendBack,indexName);
			}
			
			return ElasticSearchHandler.FindSimilar(uri,"all",indexName);
		}
		
		throw new Exception("Required parameter 'uri' is null");

		
	}
	
	
	@GET
	@Path("/similar/images")
	public String FindImagesAndUpdateES(@QueryParam("uri") String uri, @QueryParam("index") String indexName) throws Exception{
		
		if(uri != null){
			
			//initialize Image Similarity Parameters
			Initialize();
			
			//set credentials
			CredentialsProvider credsProvider = new BasicCredentialsProvider();
	        credsProvider.setCredentials(
	                new AuthScope(AuthScope.ANY_HOST, AuthScope.ANY_PORT),
	                new UsernamePasswordCredentials( imageSimilarityUserName, imageSimilarityPassword));
			
			//accept self signed certificate for https requests
			SSLContextBuilder builder = new SSLContextBuilder();
			builder.loadTrustMaterial(null, new TrustSelfSignedStrategy());
			SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(builder.build(),SSLConnectionSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
			CloseableHttpClient httpclient = HttpClients.custom().setSSLSocketFactory(sslsf)
																.setDefaultCredentialsProvider(credsProvider)
																.build();
			
			HttpGet httpGet = null;
			
			
			if(!imageSimilarityPort.equals("-1")){//if port is specified, in other words not 80
				 httpGet = new HttpGet(imageSimilarityProtocol+"://" + imageSimilarityHost + ":" + imageSimilarityPort+"/getSimilar.php?url="+uri+"&fast=1");
			}else{
				httpGet = new HttpGet(imageSimilarityProtocol+"://"+ imageSimilarityHost + "/getSimilar.php?url="+uri+"&fast=1");
			}
				
			CloseableHttpResponse httpResponse = httpclient.execute(httpGet);
			JSONObject jResults = new JSONObject();
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
											jResults = ElasticSearchHandler.UpdateWebPagesWithSimilarImages((JSONArray) jCachedImageUrls,uri,indexName);
											
										}
									}
								}
								
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
												jResults = ElasticSearchHandler.UpdateWebPagesWithSimilarImages((JSONArray) jCachedImageUrls,uri,indexName);
												
											}
										}
									}
									
								}
							}
							
						}
						httpclient.close();
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
