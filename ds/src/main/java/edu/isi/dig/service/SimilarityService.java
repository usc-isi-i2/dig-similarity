package edu.isi.dig.service;


import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

import edu.isi.dig.elasticsearch.ElasticSearchHandler;

@Path("/")
public class SimilarityService {
	
	@GET
	@Path("/similar/ads")
	public String FindSimilarAds(@QueryParam("uri") String uri,@QueryParam("return") String sendBack){
		
		if(sendBack!=null && (sendBack.equalsIgnoreCase("uri") || sendBack.equalsIgnoreCase("all"))){
			
			return ElasticSearchHandler.FindSimilar(uri,sendBack);
		}

		return ElasticSearchHandler.FindSimilar(uri,"all");
	}

}
