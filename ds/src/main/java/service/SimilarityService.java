package service;


import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

import elasticsearch.ElasticSearchHandler;

@Path("/")
public class SimilarityService {
	
	final String INDEX_NAME = "dig";
	final String DOCUMENT_TYPE = "WebPage";
	
	@GET
	@Path("/similar/ads")
	public String FindSimilarAds(@QueryParam("uri") String uri,@QueryParam("return") String sendBack){
		
		if(sendBack!=null && (sendBack.equalsIgnoreCase("uri") || sendBack.equalsIgnoreCase("all"))){
			
			return ElasticSearchHandler.FindSimilar(uri, INDEX_NAME, DOCUMENT_TYPE, sendBack);
		}

		return ElasticSearchHandler.FindSimilar(uri, INDEX_NAME, DOCUMENT_TYPE,"all");
	}

}
