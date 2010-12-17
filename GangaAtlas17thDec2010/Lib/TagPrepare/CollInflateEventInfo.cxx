/**
 * @file CollSplitByGUID.cpp
 * @brief Utility to list the file GUIDs used by a POOL collection and split the
 * collection into sub-collections by GUID
 * @author K. Karr <Kristo.Karr@cern.ch>, C. Nicholson <Caitriana.Nicholson@cern.ch>, Marcin.Nowak@cern.ch
 * $Id: CollSplitByGUID.cpp,v 1.6 2010/03/25 09:59:49 avalassi Exp $
 */

#include "CollectionBase/ICollection.h"
#include "CollectionBase/CollectionService.h"
#include "CollectionBase/CollectionDescription.h"
#include "CollectionBase/CollectionRowBuffer.h"
#include "CollectionBase/TokenList.h"
#include "CollectionBase/ICollectionQuery.h"
#include "CollectionBase/ICollectionCursor.h"
#include "CollectionBase/ICollectionDataEditor.h"

#include "CoralBase/Attribute.h"
#include "CoralBase/MessageStream.h"

#include "POOLCore/Token.h"
#include "POOLCore/Exception.h"

#include "POOLCollectionTools/Args2Container.h"
#include "POOLCollectionTools/SrcInfo.h"
#include "POOLCollectionTools/QueryInfo.h"
#include "POOLCollectionTools/CatalogInfo.h"
#include "POOLCollectionTools/MetaInfo.h"

#include <iostream>
#include <sstream>
#include <fstream>
#include <string>
#include <map>
#include <vector>
#include <memory>   // for auto_ptr
#define AUTO_PTR  auto_ptr
#include <queue>
#include <stdexcept>

using namespace std;
using namespace athenapool;

bool guidPairSort( const pair<string, int> &p1, const pair<string, int> &p2){
  return p1.second > p2.second;
}

// map GUID->output collection name (read from guid list file)
std::map<std::string,std::string>  CollNameforGuidMap;

// sequence counter to generate unique output collection names
int     CollSeqNumber = 1;


std::string generateNextCollName( )
{
  std::stringstream collectionNameStream;
  collectionNameStream << "sub_collection_" << CollSeqNumber++;
  return collectionNameStream.str();
}

bool readGuidList( const std::string& filename )
{
  ifstream     file( filename.c_str() );
  const int    linelen = 1000;
  char         line[linelen];

  string collectionName;
  while( file.good() ) {
    file.getline(line, linelen);
    char *p = line;
    while( *p == ' ' || *p == '\t' ) p++;
    if( *p == 0 || *p == '#' )
      continue;
    if( *p == ':' ) {
      char *q = ++p;
      while( *q != 0 ) q++;
      while( *(q-1) == ' ' || *(q-1) == '\t' )  q--;
      collectionName = string(p,q);
      if( collectionName.empty() ) {
        // need to generate a default output collection name here, so the GUIDs are properly grouped
        collectionName = generateNextCollName();
      }
      continue;
    }
    char *q = p;
    while( *q != 0 ) q++;
    while( *(q-1) == ' ' || *(q-1) == '\t' )  q--;
    string guid = string(p,q);
    CollNameforGuidMap[ guid ] = collectionName;
  }
  if( file.bad() ) {
    cerr << "Warning: problem reading input file <" << filename
         << "> " << endl;
    file.close();
    return false;
  }

  file.close();
  return true;
}




/*
  string guid:  GUID to look up in the user GUID list
*/
std::string collectionNameForGuid( const std::string& guid )
{
  std::map<std::string,std::string>::const_iterator i = CollNameforGuidMap.find( guid );
  if( i != CollNameforGuidMap.end() ) {
    return i->second;
  }
  return "";
}


class EventInfoCollection
{
  typedef std::map< int, std::vector<int> > oidEvtVecMap_t;
  typedef std::map< std::string, oidEvtVecMap_t > guidOidMap_t;

public:
  EventInfoCollection( ) {}
  ~EventInfoCollection() {}
  
  void addEvent( const pool::Token *tok ) {
    // add this event info to the list
    m_infoMap[ tok->dbID() ][ tok->oid().first ].push_back( tok->oid().second );
  }

  void addEvent( const char *guid, int oid, int evt ) {
    // add this event info to the list
    m_infoMap[ guid ][ oid ].push_back( evt );
  }

  void write( const char *outf ) {
    // write the info
    char buf[200];

    if (CollNameforGuidMap.size() != 0)
      {
	// store according to the guid file
	std::map<std::string,std::string>::iterator it;
	std::map<std::string,std::vector<std::string> >  InvCollNameforGuidMap;

	for (it = CollNameforGuidMap.begin(); it != CollNameforGuidMap.end(); it++)
	  {
	    InvCollNameforGuidMap[ it->second ].push_back( it->first );
	  }

	std::map<std::string,std::vector<std::string> >::iterator it2;
	for (it2 = InvCollNameforGuidMap.begin(); it2 != InvCollNameforGuidMap.end(); it2++)
	  {
	    //std::cout << it2->first << std::endl;
	    std::ofstream ofout(it2->first.c_str());
	    guidOidMap_t::iterator it3;
	    
	    // NOTE - this assumes that the GUIDs listed in the guid file are present
	    // write # of GUIDs
	    sprintf(buf, "%d", it2->second.size());
	    //ofout << buf << " ";
	    ofout << it2->second.size();

	    for (int i = 0; i < it2->second.size(); i++)
	      {
		// write the guid + # of OIDs
		sprintf(buf, "%d", m_infoMap[ it2->second[i] ].size());
		//ofout << it2->second[i] << " " << buf << " ";
		ofout << it2->second[i] << m_infoMap[ it2->second[i] ].size();

		// now go over the OIDs
		oidEvtVecMap_t::iterator it3;
		for (it3 = m_infoMap[ it2->second[i] ].begin(); it3 != m_infoMap[ it2->second[i] ].end(); it3++)
		  {
		    sprintf(buf, "%d", it3->first );
		    //ofout << buf << " "; 
		    ofout <<it3->first; 
		    
		    sprintf(buf, "%d", it3->second.size() );
		    //ofout << buf << " ";
		    ofout << it3->second.size();
		    
		    for (int j = 0; j < it3->second.size(); j++)
		      {
			sprintf(buf, "%d", it3->second[j] );
			//ofout << "                        " << buf << std::endl;
			ofout << it3->second[j];
		      }
		  }
	      }
	  }
      }
    else
      {
	std::ofstream ofout(outf);
	guidOidMap_t::iterator it;
	std::cout << "Number of GUIDs:   " << m_infoMap.size() << std::endl;
	sprintf(buf, "%d", m_infoMap.size());
	//ofout << buf << " ";
	ofout << m_infoMap.size();
	
	for ( it = m_infoMap.begin(); it != m_infoMap.end(); it++)
	  {
	    // write the guid + # of OIDs
	    sprintf(buf, "%d", it->second.size());
	    //ofout << it->first << " " << buf << " ";
	    ofout << it->first << it->second.size();

	    // now go over the OIDs
	    oidEvtVecMap_t::iterator it3;
	    for (it3 = it->second.begin(); it3 != it->second.end(); it3++)
	      {
		sprintf(buf, "%d", it3->first );
		//ofout << buf << " "; 
		ofout << it3->first; 
		
		sprintf(buf, "%d", it3->second.size() );
		//ofout << buf << " ";
		ofout << it3->second.size();
		
		for (int j = 0; j < it3->second.size(); j++)
		  {
		    sprintf(buf, "%d", it3->second[j] );
		    //ofout << "                        " << buf << std::endl;
		    ofout << it3->second[j];
		  }
	      }
	  }
	
	ofout.close();
      }
  }

  void inflate(const char *fname)
  {
    std::ifstream ifout(fname, ios::in | ios::binary);
    guidOidMap_t::iterator it;

    // prepare the output collection
    pool::ICollection* subColl = 0;
    pool::CollectionService   collectionService;
    pool::ICollection *collection = collectionService.handle( "template", "RootCollection", "", true);
    const pool::ICollectionDescription &nextDesc = collection->description();
    pool::CollectionDescription newDestDesc( nextDesc );
    
    char outName[200];
    sprintf(outName, "outColl");
    newDestDesc.setName(outName);
    subColl = collectionService.create( newDestDesc, true );
    subColl->dataEditor().setRowCacheSize( 0 );
    pool::CollectionRowBuffer rowBuffer = subColl->dataEditor().rowBuffer();

    // read type
    int ref;
    ifout.read((char*)&ref, sizeof(int));

    // read # of GUIDs
    guidOidMap_t::size_type num_guids;
    ifout.read((char*)&num_guids, sizeof(guidOidMap_t::size_type));
    std::cout << "Inflating " << num_guids << " GUIDS...\n";

    // loop over these guids
    for (int i = 0; i < num_guids; i++)
      {
	// read the guid
	char guid[37];
	ifout.read(guid, sizeof(char) * 36);
	guid[36] = '\0';
	std::cout << "    Found GUID: " << guid << std::endl;

	oidEvtVecMap_t::size_type num_oids;
	ifout.read((char*)&num_oids, sizeof(oidEvtVecMap_t::size_type));
	//std::cout << "        Found " << num_oids << " OIDs\n";
	
	for (int j = 0; j < num_oids; j++)
	  {
	    int oid;
	    ifout.read((char*)&oid, sizeof(int));
	    //std::cout << "            Found OID Number " << oid << std::endl;

	    std::vector<int>::size_type num_evts;
	    ifout.read((char*)&num_evts, sizeof(std::vector<int>::size_type));
	    //std::cout << "                Found " << num_evts << " events\n";

	    for (int k = 0; k < num_evts; k++)
	      {
		int evt;
		ifout.read((char*)&evt, sizeof(int));
		
		char buf[500];
		sprintf(buf, "[DB=%s][CNT=POOLContainer_DataHeader][CLID=D82968A1-CF91-4320-B2DD-E0F739CBC7E6][TECH=00000200][OID=%.8X-%.8X]", guid, oid, evt);
		//std::cout << buf << std::endl;
		rowBuffer.attributeList()["EventNumber"].setValue((unsigned int)evt);
		rowBuffer.attributeList()["RunNumber"].setValue((unsigned int)oid);

		if (ref == 1)
		  rowBuffer.tokenList()[ "StreamAOD_ref" ].fromString( buf );
		else
		  rowBuffer.tokenList()[ "StreamESD_ref" ].fromString( buf );
		subColl->dataEditor().insertRow( rowBuffer );
	      }	    
	  }

      }

    subColl->commit();
    subColl->close();

    ifout.close();
  }

private:

  guidOidMap_t m_infoMap;
};


int main(int argc, const char *argv[])
{
  string thisProgram("CollInflateEventInfo");

  // take all input files and inflate
  for( int i=1; i<argc; ++ i )
    {
      EventInfoCollection inInfo;
      std::cout << argv[i] << std::endl;
      inInfo.inflate( argv[i] );      
    }

  return 0;

  // create the output info object
  EventInfoCollection outInfo;

  // Convert argv to vector of strings
  vector<string> argv_v;
  for( int i=0; i<argc; ++ i )
    argv_v.push_back( argv[i] );

  try
  {
    int  maxSplit( 500 );

    bool noMetadata = false;
    // src collection info
    std::vector<int> srcCountVec;
    // dst collection info
    std::vector<bool> dstCollExistVec;
    // output modifiers
    long long minEvents = -1;
    //unsigned int numEventsPerCommit = 10000;
    unsigned rowsCached = 1000;
    unsigned int numEventsPerCommit = static_cast<unsigned int>(-1);
    int numRowsCached = 0;
    std::vector<std::string> inputQuery;
    map<string,string> srcMetadata;

    coral::MessageStream log( thisProgram );
    pool::CollectionService   collectionService;

    // vector of CmdLineArg objects
    Args2Container argsVec(thisProgram,true);
    argsVec.desc << thisProgram << " is a tool for querying an event collection, or "
                 << "collections, and storing the results in a number of output collections, "
                 << "one for each different event file GUID. " << endl
                 << "Currently, these sub-collections are output as LOCAL, "
                 << "ROOT-based collections with a fixed name and will appear in the directory from "
                 << "which the program was executed." << endl;

    // list of CollAppend *specific* cli keys and their argument properties
    QualList markers;
    markers.insert( make_pair("-guidfile",  ArgQual()) );
    markers.insert( make_pair("-maxsplit",  ArgQual()) );
    markers.insert( make_pair("-rowscached",  ArgQual()) );
    markers.insert( make_pair("-minevents",    ArgQual()) );
    markers.insert( make_pair("-nevtpercommit",ArgQual()) );
    markers.insert( make_pair("-nevtcached",   ArgQual()) );
    markers.insert( make_pair("-nometadata",   ArgQual(0)) );
    markers.insert( make_pair("-splitref",     ArgQual()) );

    markers["-guidfile"].desc << "List of GUIDs for output collections. One GUID per line. Lines starting with ':' assign collection name for GUIDS that follow";
    markers["-maxsplit"].desc << "Limit number of produced subcollections. Spillover will be stored in the last collection";
    markers["-rowscached"].desc << "Number of rows cached in memory for each output collection that is not in open collections pool. Bigger cache may speed up writing, but uses more memory. DEFAULT=" << rowsCached;
    markers["-splitref"].desc << "Name of ref to use for boundaries of split (DEFAULT=primary ref)";
    markers["-minevents"].desc << "minimum number of events required to create a separate output collection for a particular GUID";
    markers["-nevtpercommit"].desc << "Max. number of events to process between "
                                   << "output transaction commits (default is infinity)";
    markers["-nevtcached"].desc << "size of the insert buffer for bulk operations "
                                << "DEFAULT = 0 (no bulk operations)";
    markers["-nometadata"].desc << "Ignore source metadata. (will still accept new metadata from the command line -metadata option)";


    CmdLineArgs2 cmdLineArgs;
    cmdLineArgs.setArgQuals(markers);
    argsVec.push_back(&cmdLineArgs);  // Add it to the list

    // Classes with shared cli keys and their argument properties
    // Add them to the list
    CatalogInfo catinfo; argsVec.push_back(&catinfo);
    QueryInfo queryinfo; argsVec.push_back(&queryinfo);
    // DstInfo dstinfo; argsVec.push_back(&dstinfo);    // not supported yet
    SrcInfo srcinfo; argsVec.push_back(&srcinfo);
    MetaInfo metainfo; argsVec.push_back(&metainfo);

    // Print out help if requested
    if( argc < 3 ) {
      if (argc==2 && argv_v[1]=="-help") argsVec.printHelp(true);
      else  argsVec.printHelp(false);
      return 1;
    }

    // Check that all cmd line args are valid
    argsVec.evalArgs(argv_v);
    if (!argsVec.checkValid()) return 1;

    std::string splitRef = "";
    // Fill appropriate vectors based on CollAppend *specific* cmdLineArgs
    map < string, pair <int,int> >::iterator mit=cmdLineArgs.begin();
    // bool had_copymode = false;
    while( mit != cmdLineArgs.end() ) {
      string qual = mit->first;
      int ifirst = mit->second.first;
      // cout << "**********  processing option: " << qual << " " << argv_v[ifirst] << endl;
      //int ilast  = mit->second.second;
      //cout << "Parsing " << qual << " " << ifirst << " " << ilast << endl;
      if( qual == "-maxsplit" ) {
        istringstream  str( argv_v[ifirst] );
        str >> maxSplit;
        if( maxSplit < 1 )  maxSplit = 1;
      }
      else if (qual == "-rowscached") {
        istringstream  str( argv_v[ifirst] );
        str >> rowsCached;
      }
      else if (qual == "-minevents") {
        istringstream  str( argv_v[ifirst] );
        str >> minEvents;
      }
      else if (qual == "-nometadata") {
        noMetadata = true;
      }
      else if (qual == "-guidfile") {
        if( !readGuidList( argv_v[ifirst] ) ) {
          exit( -5 );
        }
      }
      else if (qual == "-splitref") {
        splitRef = argv_v[ifirst];
      }
      // ---
      // EXPERT OPTIONS
      // For tuning purposes the number of events between commits can be specified
      // ---
      else if (qual == "-nevtpercommit") {
        numEventsPerCommit = atoi( argv[ifirst] );
      }
      else if (qual == "-nevtcached") {
        numRowsCached = atoi( argv[ifirst] );
      }
      else {
        log << coral::Error << "Unrecognized option: " << qual << endl
            << "Use -help for option list" << coral::MessageStream::endmsg;
        exit(-1);
      }

      ++mit;
    }

    catinfo.setCatalogs( &collectionService );

    std::map<int, pool::ICollection*> collMap;
    // for each input collection, loop through events and write out sub-collections
    for( unsigned int i=0; i<srcinfo.nSrc(); i++ )
    {
      log << coral::Info
          << "Opening source collection " << srcinfo.name(i)
          << " of type " << srcinfo.type(i) <<  coral::MessageStream::endmsg;
      pool::ICollection *collection = collectionService.handle( srcinfo.name(i), srcinfo.type(i), srcinfo.connect(), true );
      collMap[i] = collection;

      const pool::ICollectionDescription &firstDesc = collMap[0]->description();
      const pool::ICollectionDescription &nextDesc = collection->description();

      if( (queryinfo.query() != "" || queryinfo.queryOptions() != "") && !nextDesc.equals(firstDesc) ) {
        log << coral::Warning << " The schemas of one or more "
            << "input collections are different and a query has been "
            << "requested. This may lead to unexpected behavior."
            << coral::MessageStream::endmsg;
      }

      pool::ICollectionQuery *collQuery2 = collection->newQuery();
      collQuery2->setCondition( queryinfo.query() );
      if( queryinfo.queryOptions().size() ) {
        collQuery2->addToOutputList( queryinfo.queryOptions() );
      } else {
        collQuery2->selectAll();
      }

      // second loop: write events to correct sub-collection
      pool::ICollectionCursor& cursor2 = collQuery2->execute();
      const std::string srcRefName = collection->description().eventReferenceColumnName();
      // Token name to split on (if not specified, use default for each source coll)
      const std::string splitRefName = ( splitRef.size()? splitRef : srcRefName );

      while( cursor2.next() ) {

        // get the token list from event
        const pool::TokenList &tokens = cursor2.currentRow().tokenList();

	// go through the token and save the appropriate info
	outInfo.addEvent( &(tokens[ splitRefName ]) );
      }

      log << coral::Info << "Finished storing all events from input collection " << collection->description().name() << coral::MessageStream::endmsg;

    } // for each input collection

    // create the data files
    outInfo.write("out.dat");
    return 0;
  }
  catch( pool::Exception& poolException )
  {
    std::cerr << "pool::Exception: " << poolException.what() << std::endl;
    return 1;
  }
  catch( std::exception& exception )
  {
    std::cerr << "std::Exception: " << exception.what() << std::endl;
    return 1;
  }
  catch( ... )
  {
    std::cerr << "Unknown exception. " << std::endl;
    return 1;
  }

}

