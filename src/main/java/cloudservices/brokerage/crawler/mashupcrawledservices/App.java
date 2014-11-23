package cloudservices.brokerage.crawler.mashupcrawledservices;

import cloudservices.brokerage.commons.utils.file_utils.DirectoryUtil;
import cloudservices.brokerage.commons.utils.logging.LoggerSetup;
import cloudservices.brokerage.crawler.crawlingcommons.model.DAO.BaseDAO;
import cloudservices.brokerage.crawler.crawlingcommons.model.DAO.DAOException;
import cloudservices.brokerage.crawler.crawlingcommons.model.DAO.WSDLDAO;
import cloudservices.brokerage.crawler.crawlingcommons.model.DAO.v2.RawCrawledServiceDAO;
import cloudservices.brokerage.crawler.crawlingcommons.model.entities.WSDL;
import cloudservices.brokerage.crawler.crawlingcommons.model.entities.v2.RawCrawledService;
import cloudservices.brokerage.crawler.crawlingcommons.model.enums.v2.RawCrawledServiceColType;
import cloudservices.brokerage.crawler.crawlingcommons.model.enums.v2.RawCrawledServiceType;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
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

public class App {

    private final static Logger LOGGER = Logger.getLogger(App.class.getName());
    private final static String TOKEN = ";;;";
    private final static String STAG_DS_CSV_ADDRESS = "DataSets/STag1.0/Service.csv";
    private final static String STAG_DS_ADDRESS = "DataSets/STag1.0/WSDLs";
    private final static String WSDREAM_QOS_DS_ADDRESS = "DataSets/WSDream-QoSDataset2/wslist.txt";
    private static int updatedNum = 0;
    private static int savedNum = 0;
    private static int wsdlFoundNum = 0;

    public static void main(String[] args) {
        createLogFile();
//        createNewDB();

//        copyWSDLFromV1();
//
//        LOGGER.log(Level.INFO, "***AFTER COPY WSDLS FROM V1***");
//        LOGGER.log(Level.INFO, "Number of WSDLs Found: {0}", wsdlFoundNum);
//        LOGGER.log(Level.INFO, "Number of Services Updated: {0}", updatedNum);
//        LOGGER.log(Level.INFO, "Number of Services Saved: {0}", savedNum);
//
//        wsdlFoundNum = 0;
//        updatedNum = 0;
//        savedNum = 0;
//
//        addWSDreamDS(WSDREAM_QOS_DS_ADDRESS);
//
//        LOGGER.log(Level.INFO, "***AFTER ADDING WS Dream QoS Repository***");
//        LOGGER.log(Level.INFO, "Number of WSDLs Found: {0}", wsdlFoundNum);
//        LOGGER.log(Level.INFO, "Number of Services Updated: {0}", updatedNum);
//        LOGGER.log(Level.INFO, "Number of Services Saved: {0}", savedNum);
//        wsdlFoundNum = 0;
//        updatedNum = 0;
//        savedNum = 0;
////      This has  ... problems!
//        addSTagDS(STAG_DS_CSV_ADDRESS);
//
//        LOGGER.log(Level.INFO, "***AFTER ADDING STag Repository***");
//        LOGGER.log(Level.INFO, "Number of WSDLs Found: {0}", wsdlFoundNum);
//        LOGGER.log(Level.INFO, "Number of Services Updated: {0}", updatedNum);
//        LOGGER.log(Level.INFO, "Number of Services Saved: {0}", savedNum);
        wsdlFoundNum = 0;
        updatedNum = 0;
        savedNum = 0;

        addSTagDS(STAG_DS_CSV_ADDRESS, STAG_DS_ADDRESS);

        LOGGER.log(Level.INFO, "***AFTER ADDING STag Repository***");
        LOGGER.log(Level.INFO, "Number of WSDLs Found: {0}", wsdlFoundNum);
        LOGGER.log(Level.INFO, "Number of Services Updated: {0}", updatedNum);
        LOGGER.log(Level.INFO, "Number of Services Saved: {0}", savedNum);
    }

    private static void createNewDB() {
        try {
            Configuration configuration = new Configuration();
            configuration.configure("v2hibernate.cfg.xml");
            BaseDAO.openSession(configuration);
            LOGGER.log(Level.INFO, "Database Creation Successful");
        } finally {
            BaseDAO.closeSession();
        }
    }

    private static boolean createLogFile() {
        try {
            StringBuilder sb = new StringBuilder();
            DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH-mm");
            Calendar cal = Calendar.getInstance();
            sb.append(dateFormat.format(cal.getTime()));
            String filename = sb.toString();
            DirectoryUtil.createDir("logs");
            LoggerSetup.setup("logs/" + filename + ".txt", "logs/" + filename + ".html", Level.FINER);
            return true;
        } catch (IOException ex) {
            LOGGER.log(Level.SEVERE, ex.getMessage(), ex);
            return false;
        }
    }

    private static void copyWSDLFromV1() {
        try {
            Configuration v1Configuration = new Configuration();
            v1Configuration.configure("v1hibernate.cfg.xml");
            Configuration v2Configuration = new Configuration();
            v2Configuration.configure("v2hibernate.cfg.xml");

            LOGGER.log(Level.INFO, "Starting to Migrate from Version 1 Repository...");

            WSDLDAO wsdlDAO = new WSDLDAO();
            WSDLDAO.openSession(v1Configuration);

            List wsdls = wsdlDAO.getAll("WSDL");

            RawCrawledServiceDAO crawledServiceDAO = new RawCrawledServiceDAO();
            RawCrawledServiceDAO.openSession(v2Configuration);

            for (Object wsdlObj : wsdls) {
                WSDL wsdl = (WSDL) wsdlObj;
                RawCrawledService rcs = ConvertWSDLToCrawledService(wsdl);
                wsdlFoundNum++;
                LOGGER.log(Level.FINE, "Saving or Updating Raw Crawled Service with URL = {0}", rcs.getUrl());
                addOrUpdateCrawledService(rcs, crawledServiceDAO);
            }
            LOGGER.log(Level.INFO, "Migration from Version 1 Repository Successful");
        } catch (DAOException ex) {
            LOGGER.log(Level.SEVERE, "ERROR in reading v1 database", ex);
        } finally {
            RawCrawledServiceDAO.closeSession();
        }
    }

    private static RawCrawledService ConvertWSDLToCrawledService(WSDL wsdl) {
        LOGGER.log(Level.INFO, "Converting wsdl with Id = {0}", wsdl.getId());
        RawCrawledService rcs = new RawCrawledService();
        rcs.setDescription(wsdl.getDescription());
        rcs.setTitle(wsdl.getTitle());
        rcs.setType(RawCrawledServiceType.WSDL);
        rcs.setUrl(wsdl.getUrl());
        String query = wsdl.getQuery();
        String source = "Google Crawler";
        if (query.compareTo("Service-Repository;;;XMethods") == 0
                || query.compareTo("XMethods;;;Service-Repository") == 0) {
            source = "Service-Repository;;;XMethods";
            query = "";
        } else if (query.compareTo("Service-Repository") == 0) {
            source = "Service-Repository";
            query = "";
        } else if (query.compareTo("XMethods") == 0) {
            source = "XMethods";
            query = "";
        }
        if (query.contains("Service-Repository")) {
            query = query.replace("Service-Repository;;;", ""); // if it is at first or middle
            query = query.replace(";;;Service-Repository", ""); // if it is at the end
            source = source.concat(TOKEN).concat("Service-Repository");
        }
        if (query.contains("XMethods")) {
            query = query.replace("XMethods;;;", ""); // if it is at first or middle
            query = query.replace(";;;XMethods", ""); // if it is at the end
            source = source.concat(TOKEN).concat("XMethods");
        }
        rcs.setQuery(query);
        rcs.setSource(source);
        rcs.setUpdated(true);
        return rcs;
    }

    private static void addOrUpdateCrawledService(RawCrawledService rcs, RawCrawledServiceDAO crawledServiceDAO) throws DAOException {
        RawCrawledService inDB = crawledServiceDAO.findByUrl(rcs.getUrl());

        if (inDB == null) {
            LOGGER.log(Level.FINE, "There is no raw crawled service in DB, Saving a new one");
            crawledServiceDAO.addCrawledService(rcs);
            savedNum++;
        } else {
            LOGGER.log(Level.FINE, "Found the same url with ID = {0} in DB, Trying to update", inDB.getId());

            if (inDB.getTitle().compareTo(rcs.getTitle()) != 0) {
                LOGGER.log(Level.FINER, "Titles are different;new one: {0} , indb: {1}", new Object[]{rcs.getTitle(), inDB.getTitle()});
                String[] titles = rcs.getTitle().split(TOKEN);
                for (String title : titles) {
                    if (!inDB.getTitle().contains(title)) {
                        String newString = inDB.getTitle().concat(TOKEN).concat(title);
                        LOGGER.log(Level.FINER, "Adding Title: {0}", title);
                        if (RawCrawledService.checkLength(newString.length(), RawCrawledServiceColType.TITLE)) {
                            inDB.setTitle(newString);
                        } else {
                            LOGGER.log(Level.WARNING, "Title can not be updated because it is too large!");
                        }
                    }
                }
                inDB.setUpdated(true);
            }

            if (inDB.getDescription().compareTo(rcs.getDescription()) != 0) {
                LOGGER.log(Level.FINER, "Descriptions are different;new one: {0} , indb: {1}", new Object[]{rcs.getDescription(), inDB.getDescription()});
                String[] descriptions = rcs.getDescription().split(TOKEN);
                for (String str : descriptions) {
                    if (!inDB.getDescription().contains(str)) {
                        String newString = inDB.getDescription().concat(TOKEN).concat(str);
                        LOGGER.log(Level.FINER, "Adding Description: {0}", str);
                        if (RawCrawledService.checkLength(newString.length(), RawCrawledServiceColType.DESCRIPTION)) {
                            inDB.setDescription(newString);
                        } else {
                            LOGGER.log(Level.WARNING, "Description can not be updated because it is too large!");
                        }
                    }
                }
                inDB.setUpdated(true);
            }

            if (inDB.getSource().compareTo(rcs.getSource()) != 0) {
                LOGGER.log(Level.FINER, "Sources are different;new one: {0} , indb: {1}", new Object[]{rcs.getSource(), inDB.getSource()});
                String[] newOnes = rcs.getSource().split(TOKEN);
                for (String str : newOnes) {
                    if (!inDB.getSource().contains(str)) {
                        String newString = inDB.getSource().concat(TOKEN).concat(str);
                        LOGGER.log(Level.FINER, "Adding Source: {0}", str);
                        if (RawCrawledService.checkLength(newString.length(), RawCrawledServiceColType.SOURCE)) {
                            inDB.setSource(newString);
                        } else {
                            LOGGER.log(Level.WARNING, "Source can not be updated because it is too large!");
                        }
                    }
                }
                inDB.setUpdated(true);
            }

            if (inDB.getQuery().compareTo(rcs.getQuery()) != 0) {
                LOGGER.log(Level.FINER, "Queries are different;new one: {0} , indb: {1}", new Object[]{rcs.getQuery(), inDB.getQuery()});
                String[] newOnes = rcs.getQuery().split(TOKEN);
                for (String str : newOnes) {
                    if (!inDB.getQuery().contains(str)) {
                        String newString = inDB.getQuery().concat(TOKEN).concat(str);
                        LOGGER.log(Level.FINER, "Adding Query: {0}", str);
                        if (RawCrawledService.checkLength(newString.length(), RawCrawledServiceColType.SEARCHED_QUERY)) {
                            inDB.setQuery(newString);
                        } else {
                            LOGGER.log(Level.WARNING, "Query can not be updated because it is too large!");
                        }
                    }
                }
                inDB.setUpdated(true);
            }

            if (inDB.getExtraContext().compareTo(rcs.getExtraContext()) != 0) {
                LOGGER.log(Level.FINER, "Extra context are different;new one: {0} , indb: {1}", new Object[]{rcs.getExtraContext(), inDB.getExtraContext()});
                String[] newOnes = rcs.getExtraContext().split(TOKEN);
                for (String str : newOnes) {
                    if (!inDB.getExtraContext().contains(str)) {
                        String newString = inDB.getExtraContext().concat(TOKEN).concat(str);
                        LOGGER.log(Level.FINER, "Adding Extra Context: {0}", str);
                        if (RawCrawledService.checkLength(newString.length(), RawCrawledServiceColType.EXTRA_CONTEXT)) {
                            inDB.setExtraContext(newString);
                        } else {
                            LOGGER.log(Level.WARNING, "Extra context can not be updated because it is too large!");
                        }
                    }
                }
                inDB.setUpdated(true);
            }

            if (inDB.isUpdated()) {
                crawledServiceDAO.saveOrUpdate(inDB);
                updatedNum++;
            }
        }
    }

    private static void addSTagDSFromCSV(String sTagCSVAddress) {
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
                    wsdlFoundNum++;
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
                    addOrUpdateCrawledService(rcs, crawledServiceDAO);
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

    private static void addWSDreamDS(String dsAddress) {
        LOGGER.log(Level.INFO, "Starting to Add WS Dream QoS Repository...");

        File repository = new File(dsAddress);
        BufferedReader br = null;
        String line;
        String splitBy = "" + (char) 9;

        try {

            br = new BufferedReader(new FileReader(repository));
            RawCrawledService rcs;

            Configuration v2Configuration = new Configuration();
            v2Configuration.configure("v2hibernate.cfg.xml");
            RawCrawledServiceDAO crawledServiceDAO = new RawCrawledServiceDAO();
            RawCrawledServiceDAO.openSession(v2Configuration);

            while ((line = br.readLine()) != null) {
                String[] row = line.split(splitBy);
                // at least url is neccessary
                if (row.length > 1) {
                    rcs = new RawCrawledService();
                    wsdlFoundNum++;
                    rcs.setSource("WSDreamQoS");
                    rcs.setUrl(row[1]);
                    rcs.setUpdated(true);
                    rcs.setType(RawCrawledServiceType.WSDL);
                    LOGGER.log(Level.FINE, "Saving or Updating Raw Crawled Service with URL = {0}", rcs.getUrl());
                    addOrUpdateCrawledService(rcs, crawledServiceDAO);
                }
            }
            LOGGER.log(Level.INFO, "Addition from WS Dream QoS Repository Successful");
        } catch (FileNotFoundException e) {
            LOGGER.log(Level.SEVERE, "WS Dream QoS File Not Found", e);
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "WS Dream QoS File IO Exception", e);
        } catch (DAOException ex) {
            LOGGER.log(Level.SEVERE, "ERROR in Accessing Database", ex);
        } finally {
            BaseDAO.closeSession();
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    LOGGER.log(Level.SEVERE, "WS Dream QoS File IO Exception", e);
                }
            }
        }
    }

    private static void addSTagDS(String sTagCSVAddress, String sTagAddress) {
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
                            rcs.setTitle(rcs.getTitle().concat(TOKEN).concat(infoRCS.getTitle()));
                        }
                        rcs.setExtraContext(rcs.getExtraContext().concat(TOKEN).concat(infoRCS.getExtraContext()));
                    }

                    wsdlFoundNum++;
                    LOGGER.log(Level.FINE, "Saving or Updating Raw Crawled Service with URL = {0}", rcs.getUrl());
                    addOrUpdateCrawledService(rcs, crawledServiceDAO);
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

    private static String getURLFromWSDLComments(String comments) {
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

    private static String getSeekdaComments(Node node) {
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
