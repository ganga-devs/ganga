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


/* This is a utility class to prevent running out of memory when too many
   output collections are created.
   It keeps a "window" of open collections, closing "old" ones when a new
   one needs to be opened and reopening them on demand.
   Rows are cached up to a certain limit if the collection is not opened.
*/
class CollectionPool
{
  typedef  std::vector< pool::CollectionRowBuffer >  rowVect_t;


public:
  CollectionPool( unsigned maxOpen=50, unsigned cacheSize=100 )  {
    m_maxOpen = ( maxOpen>1? maxOpen : 2 );
    m_rowCacheSize = cacheSize;
  }

  ~CollectionPool() {
    std::map< pool::ICollection*, rowVect_t >::
      iterator  closeIter = m_rowCache.begin(), mapEnd = m_rowCache.end();
    while( closeIter != mapEnd ) {
      pool::ICollection *coll = closeIter->first;
      if( !coll->isOpen() && closeIter->second.size() ) {
        // reopen to write out cached rows
        coll->open();
      }
      if( coll->isOpen() ) {
        writeCache( coll );
        coll->commit();
        coll->close();
      }
      delete coll;
      ++closeIter;
    }
  }

  void addCollection( const string& guid, pool::ICollection* coll ) {
    if( m_map.find(guid) != m_map.end() )
      throw runtime_error("Attempt to overwrite GUID in collections map");
    m_map[guid] = coll;
    if( m_rowCache.find(coll) == m_rowCache.end() ) {
      // new collection
      m_rowCache[ coll ] = rowVect_t();
      m_rowCache[ coll ].reserve( m_rowCacheSize/2+1);
      if( coll->isOpen() ) {
        queueOpenColl( coll );
      }
    }
  }


  pool::ICollection* get( const string& guid ) {
    pool::ICollection* coll = m_map[guid];
    if( !coll->isOpen() ) {
      coll->open();
      queueOpenColl( coll );
    }
    return coll;
  }

  void insertRow( const std::string& guid , const pool::CollectionRowBuffer& row ) {
    pool::ICollection* coll = m_map[guid];
    if( coll->isOpen() ) {
      coll->dataEditor().insertRow( row );
    } else {
      rowVect_t &rowVect = m_rowCache[ coll ];
      rowVect.push_back( row );
      if( rowVect.size() >= m_rowCacheSize ) {
        coll->open();
        writeCache( coll );
        queueOpenColl( coll );
      }
    }
  }

  pool::CollectionRowBuffer&  getRowBuffer( const std::string&  ) {
    return m_rowBuffer;
  }

  const string& getDstRefName() {
    return m_dstRefName;
  }


protected:

  void queueOpenColl( pool::ICollection* coll ) {
    if( m_queue.empty() ) {
      // first open collection in - store a rowBuffer copy
      m_rowBuffer =  coll->dataEditor().rowBuffer();
      m_dstRefName = coll->description().eventReferenceColumnName();
    }
    if( m_queue.size() >= (size_t)m_maxOpen ) {
      reduceQueue();
    }
    m_queue.push( coll );
  }

  void reduceQueue() {
    pool::ICollection *coll = m_queue.front();
    m_queue.pop();
    writeCache( coll );
    coll->commit();
    coll->close();
  }

  void writeCache( pool::ICollection* coll ) {
    rowVect_t &rowVect = m_rowCache[ coll ];
    for( rowVect_t::const_iterator ri = rowVect.begin(), rend = rowVect.end();
         ri != rend; ++ri ) {
      coll->dataEditor().insertRow( *ri );
    }
    rowVect.clear();
  }


  unsigned  m_maxOpen;
  unsigned  m_rowCacheSize;
  std::queue< pool::ICollection* > m_queue;
  std::map< std::string, pool::ICollection* > m_map;
  std::map< pool::ICollection*, rowVect_t > m_rowCache;
  pool::CollectionRowBuffer  m_rowBuffer;
  std::string   m_dstRefName;
};




int main(int argc, const char *argv[])
{
  string thisProgram("CollSplitByGUID");

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
      std::map< std::string, int >  eventsPerGuid;
      CollectionPool   subCollMap(50, rowsCached);  // output collection manager
      std::multimap< std::string, std::string> invCollMap;  // map to keep sub-collection name --> guid

      log << coral::Info
          << "Opening source collection " << srcinfo.name(i)
          << " of type " << srcinfo.type(i) <<  coral::MessageStream::endmsg;
      bool readOnly( true );
      pool::ICollection *collection = collectionService.handle( srcinfo.name(i), srcinfo.type(i), srcinfo.connect(), readOnly );
      collMap[i] = collection;

      const pool::ICollectionDescription &firstDesc = collMap[0]->description();
      const pool::ICollectionDescription &nextDesc = collection->description();

      if( (queryinfo.query() != "" || queryinfo.queryOptions() != "") && !nextDesc.equals(firstDesc) ) {
        log << coral::Warning << " The schemas of one or more "
            << "input collections are different and a query has been "
            << "requested. This may lead to unexpected behavior."
            << coral::MessageStream::endmsg;
      }

      pool::ICollectionQuery *collQuery = collection->newQuery();
      pool::ICollectionQuery *collQuery2 = collection->newQuery();
      collQuery->setCondition( queryinfo.query() );
      collQuery2->setCondition( queryinfo.query() );
      if( queryinfo.queryOptions().size() ) {
        collQuery->addToOutputList( queryinfo.queryOptions() );
        collQuery2->addToOutputList( queryinfo.queryOptions() );
      } else {
        collQuery->selectAll();
        collQuery2->selectAll();
      }

      log << coral::Info << "Executing query for the source collection" << coral::MessageStream::endmsg;
      pool::ICollectionCursor& cursor = collQuery->execute();

      // set parameters for the sub-collections.
      // currently caters only for writing local, Root-based collections.
      std::string subCollType = "RootCollection";
      std::string subCollConnect = "";

      // first loop: for each event, find the GUID / fileId and
      // count how many events match that GUID
      int totalEvents = 0;
      int uniqueGuids = 0;

      // Token name to split on (if not specified, use default for each source coll)
      string refname = ( splitRef.size()? splitRef : collection->description().eventReferenceColumnName() );
      while( cursor.next() ) {
        const pool::TokenList &tokens = cursor.currentRow().tokenList();
        for( pool::TokenList::const_iterator iter = tokens.begin(); iter != tokens.end(); ++iter ) {
          if( iter.tokenName() == refname ) {
            string guid = iter->dbID();
            if( eventsPerGuid.find( guid ) == eventsPerGuid.end() ) {
              // new unique GUID found
              eventsPerGuid[ guid ] = 1;
              uniqueGuids++;
            }
            else {
              eventsPerGuid[ guid ]++;
            }
          }
        }
        totalEvents++;
      }
      log << coral::Info << "Collection " << srcinfo.name(i) << " has " << totalEvents
          << " entries with " << uniqueGuids << " unique file GUIDs in Token " << refname
          << coral::MessageStream::endmsg;

      //--------  make suitable output collections ( each with nEvents >= minEvents )
      int createdCollections = 0;

      vector<pair<string,int> > sortedGuids;
      map<string, pool::ICollection*> collMap;
      for( map<string,int>::iterator guidIter1 = eventsPerGuid.begin(),
             end = eventsPerGuid.end(); guidIter1 != end; ++guidIter1 )
      {
        string guid = guidIter1->first;
        string subCollName = collectionNameForGuid( guid );
        if( !subCollName.empty() )
        {
          // process guid from a list
          pool::ICollection* subCollection = 0;
          if( collMap.find( subCollName ) == collMap.end() )
          {
            // create a new collection
            pool::CollectionDescription newDestDesc( nextDesc );
            newDestDesc.setName( subCollName );
            newDestDesc.setType( subCollType );
            newDestDesc.setConnection( subCollConnect );
            subCollection = collectionService.create( newDestDesc, true );
            createdCollections++;
            subCollection->dataEditor().setRowCacheSize( 0 );
            collMap[ subCollName ] = subCollection;
          }
          else
          {
            // find an already created collection
            subCollection = collMap[ subCollName ];
          }
          // map to appropriate GUID
          subCollMap.addCollection( guid, subCollection );
          invCollMap.insert( std::pair<std::string, std::string>( subCollName, guid ) );
        }
        else
        {
          // guid not from the list, keep it for default processing
          sortedGuids.push_back( *guidIter1 );
        }
      }
      // sort the remaining GUIDs by cardinality
      sort( sortedGuids.begin(), sortedGuids.end(), guidPairSort );

      int rowCounter = 0;
      unsigned guidCounter = 0;
      pool::ICollection* subCollection = 0;
      string subCollName;
      vector<pair<string,int> >::iterator guidIter2 = sortedGuids.begin();
      while( guidIter2 != sortedGuids.end() )
      {
        guidCounter++;
        std::string guid = guidIter2->first;
        int thisNumEvents = guidIter2->second;
        bool collLimit = ( createdCollections >= maxSplit );

        // create a new output collection if
        if( !subCollection    // there is no collection yet or
            || ( rowCounter >= minEvents    // enough events were written to the previous one
                 && !collLimit ) )   // but we are not over the collection limit
        {
          // create a new sub-collection
          subCollName = generateNextCollName();
          pool::CollectionDescription newDestDesc( nextDesc );
          newDestDesc.setName( subCollName );
          newDestDesc.setType( subCollType );
          newDestDesc.setConnection( subCollConnect );

          subCollection = collectionService.create( newDestDesc, true );
          subCollection->dataEditor().setRowCacheSize( 0 );
          createdCollections++;
          rowCounter = 0;
        }
        // map to appropriate GUID
        subCollMap.addCollection( guid, subCollection );
        invCollMap.insert( std::pair<std::string, std::string>(subCollName, guid));
        rowCounter += thisNumEvents;
        ++guidIter2;
      }

      std::string lastCollName = "";
      for( std::multimap<std::string, std::string>::const_iterator invCollIter = invCollMap.begin();
           invCollIter != invCollMap.end();
           ++invCollIter)
      {
        std::string thisCollName = invCollIter->first;
        if (thisCollName == lastCollName)
          log << coral::Info << " " << invCollIter->second;
        else 
        {
          if (invCollIter != invCollMap.begin())
            log << coral::Info << coral::MessageStream::endmsg;
          log << coral::Info << "Created new sub-collection " << thisCollName << " with files:\t" << invCollIter->second;
        }
        lastCollName = thisCollName;
      }
      log << coral::Info << coral::MessageStream::endmsg;

      // second loop: write events to correct sub-collection
      pool::ICollectionCursor& cursor2 = collQuery2->execute();
      const std::string srcRefName = collection->description().eventReferenceColumnName();
      const std::string dstRefName = subCollMap.getDstRefName();
      // Token name to split on (if not specified, use default for each source coll)
      const std::string splitRefName = ( splitRef.size()? splitRef : srcRefName );
      size_t row = 0;
      while( cursor2.next() ) {
        row ++;
        // get file GUID from event
        const pool::TokenList &tokens = cursor2.currentRow().tokenList();
        string guid = tokens[ splitRefName ].dbID();
        pool::CollectionRowBuffer &rowBuffer = subCollMap.getRowBuffer( guid );
        // copy all attributes
        //rowBuffer.attributeList() = cursor2.currentRow().attributeList();
	
	//std::cout << subCollMap.getDstRefName() << std::endl;
	//if (row == 1)
	// {
	//    tokens.toOutputStream(std::cout);
	//    std::cout << std::endl;
	//  }
	//if (row < 500) continue;
	
	//if (row == 505) break;
        // copy the tokens
        for( pool::TokenList::const_iterator ti = tokens.begin(); ti != tokens.end(); ++ti ) {
	  /*if (ti.tokenName() == "StreamAOD_ref")
	    {
	      
	    }
	  else
	    continue;
	  */
          if( ti.tokenName() == srcRefName ) {
	    //std::cout<< "--  " << dstRefName << std::endl;
	    std::string tok_str = ti->toString();
	    std::cout << tok_str << std::endl;
	    rowBuffer.tokenList()[ "StreamAOD_ref" ].fromString( tok_str );
            //ti->setData( &rowBuffer.tokenList()[ dstRefName ] );
          } else {
	    std::cout<< ti.tokenName() << std::endl;
            //ti->setData( &rowBuffer.tokenList()[ ti.tokenName() ] );

	    //std::cout << ti->m_dbID <<std::endl;
	    /*	    ti->m_dbID;
	    ti->m_cntID
     184               0 :   pToken->m_classID     = m_classID;
     185               0 :   pToken->m_oid.first   = m_oid.first;z
     186               0 :   pToken->m_oid.second  = m_oid.second;
     187               0 :   pToken->m_technology  = m_technology;*/
          }
        }

	// NOTE!  next line effectively disables row cache and is a huge performance hit
	// necessary to prevent bug in pool::TokenList copy that results in Tokens switching places!
	subCollMap.get(guid);  // !!!  FIXME - remove in LCGCMT > 56d
        subCollMap.insertRow( guid, rowBuffer );
      }
      log << coral::Info << "Finished writing all events from input collection " << collection->description().name() << coral::MessageStream::endmsg;

    } // for each input collection

    for( unsigned int i=0; i< srcinfo.nSrc(); i++ ) {
      collMap[i]->close();
    }
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

