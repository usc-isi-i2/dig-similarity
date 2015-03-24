package edu.isi.dig.elasticsearch;

import java.util.concurrent.Callable;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.isi.dig.service.SimilarityService.ImageRank;


public class TermQueryWorker implements Callable<String> {
	private static Logger LOG = LoggerFactory.getLogger(TermQueryWorker.class);
	ImageRank ir;
	String indexToUse;
	String queryUri;
	public TermQueryWorker(ImageRank ir, String indexToUse, String queryUri)
	{
		this.ir = ir;
		this.indexToUse = indexToUse;
		this.queryUri = queryUri;
	}
	@Override
	public String call() throws Exception {
		try{
		StringBuilder bulkUpdate = new StringBuilder();
		HttpPost httpPostTermQuery = new HttpPost(ElasticSearchHandler.elasticsearchProtocol + "://" + ElasticSearchHandler.elasticsearchHost + ":" + ElasticSearchHandler.elasticsearchPort + "/" + indexToUse + "/_search");
		
		String termQuery = 	"{\"query\":{\"bool\" : { \"must\" : { \"term\" : { \"" + ElasticSearchHandler.IMAGE_CACHE_URL + "\"" + ":\"" + ir.getImageURL() + "\"}}}}}";
		

		StringEntity entity = new StringEntity(termQuery,"UTF-8");
		entity.setContentType("application/json");
		httpPostTermQuery.setEntity(entity);
		
		CloseableHttpClient httpClientTQ=null;
		if(ElasticSearchHandler.sslsf != null && ElasticSearchHandler.credsProvider != null){
			httpClientTQ = HttpClients.custom().setSSLSocketFactory(ElasticSearchHandler.sslsf)
											   .setDefaultCredentialsProvider(ElasticSearchHandler.credsProvider)
											   .build();
		}
		else{
			httpClientTQ = HttpClients.createDefault();
		}
		
		 
		HttpResponse httpResp = httpClientTQ.execute(httpPostTermQuery);
		
		
		if(httpResp != null && httpResp.getStatusLine().getStatusCode() >=200 && httpResp.getStatusLine().getStatusCode() < 300){

			
			JSONObject termQueryResponse = (JSONObject) JSONSerializer.toJSON(EntityUtils.toString(httpResp.getEntity()));
			
			if(termQueryResponse.containsKey(ElasticSearchHandler.HITS)) {
				
				JSONObject jHitsObject = termQueryResponse.getJSONObject(ElasticSearchHandler.HITS);
				
				if(jHitsObject.containsKey(ElasticSearchHandler.HITS)) {
					
					JSONArray jHitsArray = jHitsObject.getJSONArray(ElasticSearchHandler.HITS);
					
					
					for(int j=0;j<jHitsArray.size();j++){
						
						String docId = jHitsArray.getJSONObject(j).getString(ElasticSearchHandler.ID);
						
						JSONObject jUpdatedSource = ElasticSearchHandler.addSimilarImagesFeature(jHitsArray.getJSONObject(j).getJSONObject(ElasticSearchHandler.SOURCE),
								queryUri,
								                                            ir);
						
						String bulkFormat = "{\"update\":{\"_index\":\"" + indexToUse+ "\",\"_type\":\""+ ElasticSearchHandler.docType +"\",\"_id\":\""+docId+"\"}}";
						
						bulkUpdate.append(bulkFormat);
						bulkUpdate.append(System.getProperty("line.separator"));
						bulkUpdate.append("{\"doc\":");
						bulkUpdate.append(jUpdatedSource);
						bulkUpdate.append("}");
						bulkUpdate.append(System.getProperty("line.separator"));
					}
				}
			}
			
		}
		httpClientTQ.close();
		return bulkUpdate.toString();
		}
		catch(Exception e)
		{
			LOG.error("unable to do a term query", e);
			throw new Exception("Unable to do a term query", e);
		}
	}
	
}
