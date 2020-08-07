package com.linkedin.metadata.builders.graph.relationship;

import com.linkedin.common.urn.Urn;
import com.linkedin.dataset.FineGrainUpstreamFieldMapping;
import com.linkedin.dataset.FineGrainUpstream;
import com.linkedin.dataset.FineGrainUpstreamLineage;
import com.linkedin.metadata.builders.graph.GraphBuilder;
import com.linkedin.metadata.relationship.DerivedBy;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;

import static com.linkedin.metadata.dao.internal.BaseGraphWriterDAO.RemovalOption.*;


public class DerivedByBuilderFromFineGrainUpstreamLineage extends BaseRelationshipBuilder<FineGrainUpstreamLineage>  {
  public DerivedByBuilderFromFineGrainUpstreamLineage() {
    super(FineGrainUpstreamLineage.class);
  }

  @Nonnull
  @Override
  public List<GraphBuilder.RelationshipUpdates> buildRelationships(@Nonnull Urn urn, @Nonnull FineGrainUpstreamLineage upstreamLineage) {
    if (upstreamLineage.getUpstreams().isEmpty()) {
      return Collections.emptyList();
    }

    List<DerivedBy> list = new ArrayList();

    for (FineGrainUpstream stream: upstreamLineage.getUpstreams()) {
      for (FineGrainUpstreamFieldMapping upstreamFieldMapping: stream.getFields()) {
        try {
          list.add(new DerivedBy()
              .setSource(Urn.createFromString(stream.getDataset().toString() + ":" + upstreamFieldMapping.getSourceField().getFieldPath()))
              .setDestination(Urn.createFromString(urn.toString() + ":" + upstreamFieldMapping.getDestinationField().getFieldPath())));
        } catch (URISyntaxException e) {
          return null;
        }
      }
    }

    return Collections.singletonList(new GraphBuilder.RelationshipUpdates(list, REMOVE_ALL_EDGES_FROM_SOURCE));
  }

}
