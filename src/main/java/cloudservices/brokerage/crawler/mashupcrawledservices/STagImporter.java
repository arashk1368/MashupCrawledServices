/*
 * Copyright 2014 Arash khodadadi.
 * <http://www.arashkhodadadi.com/>
 */
package cloudservices.brokerage.crawler.mashupcrawledservices;

import cloudservices.brokerage.commons.utils.file_utils.DirectoryUtil;
import cloudservices.brokerage.crawler.crawlingcommons.model.DAO.BaseDAO;
import cloudservices.brokerage.crawler.crawlingcommons.model.DAO.DAOException;
import cloudservices.brokerage.crawler.crawlingcommons.model.DAO.v2.RawCrawledServiceDAO;
import cloudservices.brokerage.crawler.crawlingcommons.model.entities.v2.RawCrawledService;
import cloudservices.brokerage.crawler.crawlingcommons.model.enums.v2.RawCrawledServiceType;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.hibernate.cfg.Configuration;
import org.w3c.dom.Comment;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 *
 * @author Arash Khodadadi <http://www.arashkhodadadi.com/>
 */
public class STagImporter {
    
    private final static Logger LOGGER = Logger.getLogger(STagImporter.class.getName());

    public static void addSTagDS(String sTagCSVAddress, String sTagAddress) {
        LOGGER.log(Level.INFO, "Starting to Add STag Repository...");

        List<File> files = DirectoryUtil.getAllFiles(sTagAddress);
        LOGGER.log(Level.INFO, "{0} files found", files.size());
        RawCrawledService rcs;
        int noInfoinCSV = 0;

        Configuration v2Configuration = new Configuration();
        v2Configuration.configure("v2hibernate.cfg.xml");
        RawCrawledServiceDAO crawledServiceDAO = new RawCrawledServiceDAO();
        RawCrawledServiceDAO.openSession(v2Configuration);

        try {
            for (File file : files) {

                LOGGER.log(Level.INFO, "File : {0} found from address : {1}", new Object[]{file.getName(), file.getPath()});
                DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
                dbf.setIgnoringComments(false);
                DocumentBuilder db = dbf.newDocumentBuilder();
                Document doc = db.parse(file);
                String comments = getSeekdaComments(doc);
                if (comments.isEmpty()) {
                    comments = getSeekdaComments(doc.getDocumentElement());
                }
                LOGGER.log(Level.FINE, "Comments found: {0}", comments);
                String url = getURLFromWSDLComments(comments);
                if (url.isEmpty()) {
                    LOGGER.log(Level.FINE, "No url found");
                } else {
                    rcs = new RawCrawledService();
                    rcs.setExtraContext(file.getCanonicalPath());
                    rcs.setSource("STag");
                    String title = file.getName();
                    title = title.replace(".wsdl", "");
                    rcs.setTitle(title);
                    rcs.setType(RawCrawledServiceType.WSDL);
                    rcs.setUpdated(true);
                    rcs.setUrl(url);

                    RawCrawledService infoRCS = getInfoFromCSV(url, sTagCSVAddress);
                    if (infoRCS == null) {
                        LOGGER.log(Level.INFO, "No info found from csv file");
                        noInfoinCSV++;
                    } else {
                        rcs.setDescription(infoRCS.getDescription());
                        rcs.setQuery(infoRCS.getQuery());
                        if (rcs.getTitle().compareTo(infoRCS.getTitle()) != 0) {
                            rcs.setTitle(rcs.getTitle().concat(App.TOKEN).concat(infoRCS.getTitle()));
                        }
                        rcs.setExtraContext(rcs.getExtraContext().concat(App.TOKEN).concat(infoRCS.getExtraContext()));
                    }

                    App.ServiceFoundNum++;
                    LOGGER.log(Level.FINE, "Saving or Updating Raw Crawled Service with URL = {0}", rcs.getUrl());
                    App.addOrUpdateCrawledService(rcs, crawledServiceDAO);
                }
            }
            LOGGER.log(Level.INFO, "Addition from STag Repository Successful");
        } catch (SAXException | IOException | ParserConfigurationException ex) {
            LOGGER.log(Level.SEVERE, "ERROR IN PARSING FILE", ex);
        } catch (DAOException ex) {
            LOGGER.log(Level.SEVERE, "ERROR in Accessing Database", ex);
        } finally {
            BaseDAO.closeSession();
            LOGGER.log(Level.INFO, "For {0} urls no info found from CSV", noInfoinCSV);
        }
    }

    public static void addSTagDSFromCSV(String sTagCSVAddress) {
        LOGGER.log(Level.INFO, "Starting to Add STag Repository...");

        File csvRepo = new File(sTagCSVAddress);
        BufferedReader br = null;
        String line;
        String cvsSplitBy = "\",\"";

        try {

            br = new BufferedReader(new FileReader(csvRepo));
            br.readLine();
            RawCrawledService rcs;

            Configuration v2Configuration = new Configuration();
            v2Configuration.configure("v2hibernate.cfg.xml");
            RawCrawledServiceDAO crawledServiceDAO = new RawCrawledServiceDAO();
            RawCrawledServiceDAO.openSession(v2Configuration);

            while ((line = br.readLine()) != null) {
                String[] row = line.split(cvsSplitBy);
                // at least url is neccessary
                if (row.length > 3) {
                    rcs = new RawCrawledService();
                    App.ServiceFoundNum++;
                    rcs.setSource("STag");
                    // Name in CSV
                    rcs.setTitle(row[0].replaceAll("\"", ""));
                    rcs.setUrl(row[3].replaceAll("\"", ""));
                    if (row.length > 7) {
                        // Wiki Description in CSV
                        rcs.setDescription(row[7].replaceAll("\"", ""));
                        if (row.length > 8) {
                            // Tags in CSV
                            String tagsStr = row[8].replaceAll("\"", "");
                            // Match it to new structure
                            rcs.setQuery(tagsStr.replaceAll(",", ";;;"));
                        }
                    }
                    rcs.setUpdated(true);
                    rcs.setType(RawCrawledServiceType.WSDL);
                    LOGGER.log(Level.FINE, "Saving or Updating Raw Crawled Service with URL = {0}", rcs.getUrl());
                    App.addOrUpdateCrawledService(rcs, crawledServiceDAO);
                }
            }
            LOGGER.log(Level.INFO, "Addition from STag Repository Successful");
        } catch (FileNotFoundException e) {
            LOGGER.log(Level.SEVERE, "STag CSV File Not Found", e);
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "STag CSV File IO Exception", e);
        } catch (DAOException ex) {
            LOGGER.log(Level.SEVERE, "ERROR in Accessing Database", ex);
        } finally {
            BaseDAO.closeSession();
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    LOGGER.log(Level.SEVERE, "STag CSV File IO Exception", e);
                }
            }
        }
    }

    public static String getURLFromWSDLComments(String comments) {
        String url = "";
        if (comments.isEmpty()) {
            LOGGER.log(Level.FINER, "Comments are empty");
        } else if (!comments.contains("WSDL definition available at")) {
            LOGGER.log(Level.FINER, "Comments does not contain the cache description");
        } else {
            // Remove first enter
            comments = comments.substring(1);
            // It could be http or https
            int httpIndex = comments.indexOf("http");
            if (httpIndex == -1) {
                LOGGER.log(Level.FINER, "Comments does not contain the link");
            } else {
                int brIndex = comments.indexOf("\n");
                if (brIndex == -1) {
                    LOGGER.log(Level.FINER, "Comments does not contain break line");
                } else {
                    url = comments.substring(httpIndex, brIndex);
                    // Remove the dot
                    url = url.substring(0, url.length() - 1);
                }
            }
        }
        return url;
    }

    public static String getSeekdaComments(Node node) {
        NodeList nl = node.getChildNodes();
        for (int i = 0; i < nl.getLength(); i++) {
            if (nl.item(i).getNodeType() == Element.COMMENT_NODE) {
                Comment comment = (Comment) nl.item(i);
                if (comment.getData().contains("This is the seekda")) {
                    return comment.getData();
                }
            }
        }
        return "";
    }

    private static RawCrawledService getInfoFromCSV(String url, String sTagCSVAddress) {
        RawCrawledService rcs = null;
        LOGGER.log(Level.FINER, "Getting information from CSV file for URL= {0}", url);

        File csvRepo = new File(sTagCSVAddress);
        BufferedReader br = null;
        String line;
        String cvsSplitBy = "\",\"";
        int lineNum = 0;

        try {

            br = new BufferedReader(new FileReader(csvRepo));
            // For headers
            br.readLine();
            lineNum++;

            while ((line = br.readLine()) != null) {
                lineNum++;
                String[] row = line.split(cvsSplitBy);
                // at least url is neccessary
                if (row.length > 3) {
                    String rowUrl = row[3].replaceAll("\"", "");
                    if (!rowUrl.isEmpty()) {
                        // Remove ... from url and check two parts
                        int dotsIndex = rowUrl.indexOf("...");
                        String left = rowUrl;
                        String right = rowUrl;
                        if (dotsIndex != -1) {
                            // If ... are placed between two dots -1 is necessary!
                            left = rowUrl.substring(0, dotsIndex - 1);
                            // If ... are placed after a dot +1 is necessary!
                            right = rowUrl.substring(dotsIndex + 3 + 1, rowUrl.length());
                        }
                        if (!(left.isEmpty() && right.isEmpty()) && url.contains(left) && url.contains(right)) {
                            LOGGER.log(Level.FINER, "URL : {0} found from CSV file with URL in file : {1} in line number : {2}",
                                    new Object[]{url, rowUrl, lineNum});
                            rcs = new RawCrawledService();
                            rcs.setUrl(url);
                            rcs.setUpdated(true);
                            rcs.setType(RawCrawledServiceType.WSDL);
                            rcs.setExtraContext(String.valueOf(lineNum));
                            // Name in CSV
                            rcs.setTitle(row[0].replaceAll("\"", ""));
                            if (row.length > 7) {
                                // Wiki Description in CSV
                                String desc = row[7].replaceAll("\"", "");
                                desc = desc.replace("This provider has no wiki description yet. Help us out and write a line or two about this prvoider. Just click the edit button at the top right of this box.", "");
                                rcs.setDescription(desc);
                                if (row.length > 8) {
                                    // Tags in CSV
                                    String tagsStr = row[8].replaceAll("\"", "");
                                    // Match it to new structure
                                    rcs.setQuery(tagsStr.replaceAll(",", ";;;"));
                                }
                            }
                            break;
                        }
                    }
                }
            }
        } catch (FileNotFoundException e) {
            LOGGER.log(Level.SEVERE, "STag CSV File Not Found", e);
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "STag CSV File IO Exception", e);
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    LOGGER.log(Level.SEVERE, "STag CSV File IO Exception", e);
                }
            }
        }

        return rcs;
    }
}
