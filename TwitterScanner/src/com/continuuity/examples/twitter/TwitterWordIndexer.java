/*
 * TwitterScanner - Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */
package com.continuuity.examples.twitter;

import com.continuuity.api.data.util.Bytes;
import com.continuuity.api.flow.flowlet.ComputeFlowlet;
import com.continuuity.api.flow.flowlet.FlowletSpecifier;
import com.continuuity.api.flow.flowlet.OutputCollector;
import com.continuuity.api.flow.flowlet.Tuple;
import com.continuuity.api.flow.flowlet.TupleContext;

public class TwitterWordIndexer extends ComputeFlowlet {

  @Override
  public void configure(FlowletSpecifier specifier) {
    specifier.getDefaultFlowletInput().setSchema(
        TwitterFlow.POST_PROCESS_SCHEMA);
  }

  private SortedCounterTable topUsers;

  @Override
  public void initialize() {
    this.topUsers = getFlowletContext().getDataSet(TwitterFlow.topUsers);
  }

  @Override
  public void process(Tuple tuple, TupleContext context,
      OutputCollector collector) {

    String user = tuple.get("name");
    Long postValue = tuple.get("value");

    // Perform post-increment for top users
    topUsers.performSecondaryCounterIncrements(
        TwitterFlow.USER_SET, Bytes.toBytes(user), 1L, postValue);
    
  }

}
