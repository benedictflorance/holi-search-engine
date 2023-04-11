package cis5550.jobs;
import cis5550.tools.URLParser;

import java.net.MalformedURLException;
import java.net.URL;

public class UrlNormalizer {

    public static String normalize(String baseUrl, String link) throws MalformedURLException {
        // Parse the base URL
        String[] parsedBaseUrl = URLParser.parseURL(baseUrl);
        String protocol = parsedBaseUrl[0];
        String host = parsedBaseUrl[1];
        String port = parsedBaseUrl[2];
//        if(protocol==null) {
//        	protocol = "http";
//        }
        if(port==null) {
        	port = getDefaultPort(protocol);
        }

        // Parse the link URL
        String[] parsedLinkUrl = URLParser.parseURL(link);
        String linkProtocol = parsedLinkUrl[0];
        String linkHost = parsedLinkUrl[1];
        String linkPort = parsedLinkUrl[2];
        String linkPath = parsedLinkUrl[3];
        
        if(linkProtocol!=null)
        	if(!linkProtocol.equals("https") && !linkProtocol.equals("http")) {
        		return null;
        }
        else if(protocol!=null){
        	if(!protocol.equals("https") && !protocol.equals("http")) {
        		return null;
        	}
        }
        
        if(linkPath.endsWith("jpg")||linkPath.endsWith("jpeg")||linkPath.endsWith("png")
        		||linkPath.endsWith("gif")||linkPath.endsWith("txt")) {
        	return null;
        }

        // If the link has a fragment identifier, remove it
        int fragmentIndex = linkPath.indexOf('#');
        if (fragmentIndex != -1) {
            linkPath = linkPath.substring(0, fragmentIndex);
        }

        // If the link is empty, return null
        if (linkPath.isEmpty()) {
            return baseUrl;
        }

        // If the link is an absolute URL, return it
        if (linkHost != null) {
            if (linkPort == null) {
                linkPort = getDefaultPort(linkProtocol);
            }
            return linkProtocol + "://" + linkHost + ":" + linkPort + linkPath;
        }

        // If the link is a protocol-relative URL, return it
        if (linkPath.startsWith("//")) {
            return protocol + ":" + linkPath;
        }

        // If the link is a relative URL, resolve it against the base URL
        if(linkPath.startsWith("/"))
        	return protocol + "://" + host + ":" + port + linkPath;
        String basePath = parsedBaseUrl[3];
        int lastSlashIndex = basePath.lastIndexOf('/');
        if (lastSlashIndex != -1) {
            basePath = basePath.substring(0, lastSlashIndex + 1);
        }
        
        while (linkPath.startsWith("../")) {
            int slashIndex = basePath.substring(0, basePath.length() - 1).lastIndexOf('/');
            if (slashIndex != -1) {
                basePath = basePath.substring(0, slashIndex + 1);
            }
            linkPath = linkPath.substring(3);
        }
       
        return protocol + "://" + host + ":" + port + basePath + linkPath;
    }

    private static String getDefaultPort(String protocol) {
        switch (protocol) {
            case "http":
                return "80";
            case "https":
                return "443";
            default:
                return null;
        }
    }
}
