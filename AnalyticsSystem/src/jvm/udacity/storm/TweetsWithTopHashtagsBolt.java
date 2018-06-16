package udacity.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import udacity.storm.tools.*;
import udacity.storm.tools.Rankings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.Map;
import java.util.HashMap;
import java.util.Arrays;
import java.util.List;

/**
 * A bolt that parses the tweet into words
 */
public class TweetsWithTopHashtagsBolt extends BaseRichBolt
{
  // To output tuples from this bolt to the count bolt
  OutputCollector collector;


  // Map to store the top hash tags.
  private Map<String, Long> tagMap;


  @Override
  public void prepare(
      Map                     map,
      TopologyContext         topologyContext,
      OutputCollector         outputCollector)
  {
    // save the output collector for emitting tuples
    collector = outputCollector;
    // create and initialize the map
    tagMap = new HashMap<String, Long>();
  }

  @Override
  public void execute(Tuple tuple)
  {

    String componentId = tuple.getSourceComponent();

   if(componentId.equals("tweet-spout")) 
    {

      if(tagMap.size() == 0){
        //collector.emit(new Values("DELIMETER" + "wait a moment!" + "DELIMETER", new Long(3)));
      }
      else

      {
        String tweet = tuple.getString(0);
          // provide the delimiters for splitting the tweet
          String delims = "[ .,?!]+";

          // now split the tweet into tokens
          String[] tokens = tweet.split(delims);

          // FIXME fiund the hottest tag with tokens set and tags sorted list.
          // HashSet<String> tokensSet = new HashSet<String>(Arrays.asList(tokens));

          for (String token: tokens) {
            if(token.startsWith("#")){
              if(tagMap.containsKey(token)){
                collector.emit(new Values(tweet, tagMap.get(token)));
                break;

              }
            }
            }
      }
    }

    else if(componentId.equals("total-ranker")) 
    {
      
      tagMap.clear();
      Rankings mergedRankings = (Rankings) tuple.getValue(0);
      List<Rankable> rankables = mergedRankings.getRankings();
      String tag = new String();
      long count = 0;
      for(Rankable r: rankables){
        tag = (String)r.getObject();
        count = r.getCount();
        tagMap.put(tag, count);
        
      }

    }

    else
    {
      
    }

  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer)
  {
    // tell storm the schema of the output tuple for this spout
    // tuple consists of 'tweet-word' and count of that
    declarer.declare(new Fields("tweet-word","count"));
  }

}
