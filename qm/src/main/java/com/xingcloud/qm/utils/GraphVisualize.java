package com.xingcloud.qm.utils;

import com.jgraph.layout.JGraphFacade;
import com.jgraph.layout.hierarchical.JGraphHierarchicalLayout;
import com.jgraph.layout.tree.JGraphTreeLayout;
import org.apache.drill.common.graph.AdjacencyList;
import org.apache.drill.common.graph.Edge;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.logical.data.LogicalOperator;
import org.jgraph.JGraph;
import org.jgrapht.DirectedGraph;
import org.jgrapht.ext.JGraphModelAdapter;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleDirectedGraph;

import javax.imageio.ImageIO;
import javax.swing.*;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class GraphVisualize {
  
  private static DirectedGraph<LogicalOperator, DefaultEdge> buildDirectedGraph(LogicalPlan plan) {
    DirectedGraph<LogicalOperator, DefaultEdge> graph = new SimpleDirectedGraph<LogicalOperator, DefaultEdge>(DefaultEdge.class);
    Collection<Edge<AdjacencyList<LogicalOperator>.Node>> edges = plan.getGraph().getAdjList().getAllEdges();    
    for(Edge<AdjacencyList<LogicalOperator>.Node> edge:edges){
      graph.addVertex(edge.getFrom().getNodeValue());
      graph.addVertex(edge.getTo().getNodeValue());
      graph.addEdge(edge.getFrom().getNodeValue(), edge.getTo().getNodeValue());
    }
    return graph;
    
  }
  
  static int WIDTH = 1200;
  static int HEIGHT = 1200;
  
  public static void visualize(LogicalPlan plan, String pngPath){
    DirectedGraph<LogicalOperator, DefaultEdge> grapht = buildDirectedGraph(plan);
    // create a visualization using JGraph, via the adapter
    JGraphModelAdapter<LogicalOperator, DefaultEdge> adapter = new JGraphModelAdapter<LogicalOperator, DefaultEdge>(grapht);
    JGraph jgraph = new JGraph( adapter ); 
    jgraph.setDoubleBuffered(false);
    jgraph.setPreferredSize(new Dimension(20000, 20000));
//    jgraph.setSize(new Dimension(600,500));
//    jgraph.setLayout(new GridLayout());
    
    
    JPanel panel = new JPanel();
    panel.setDoubleBuffered(false);
    panel.add(jgraph);
//    panel.setPreferredSize(new Dimension(1600,1500));
    panel.setVisible( true ); 
    panel.setEnabled(true);
    panel.addNotify();
    panel.validate();
    JGraphFacade jgf = new JGraphFacade(jgraph);
    
//    JGraphTreeLayout treeLayout = new JGraphTreeLayout();
//    treeLayout.run(jgf);
    JGraphHierarchicalLayout hLayout = new JGraphHierarchicalLayout();
    hLayout.run(jgf);
    
    Map nestedMap = jgf.createNestedMap(true, true);
    jgraph.getGraphLayoutCache().edit(nestedMap);

    jgraph.getGraphLayoutCache().update();
    jgraph.refresh();
    
    BufferedImage img = jgraph.getImage(jgraph.getBackground(), 0);
    FileOutputStream out = null;
    try {
      out = new FileOutputStream(pngPath);
      ImageIO.write(img, "png", out);
      out.flush();
      out.close();
    } catch (FileNotFoundException e) {
      e.printStackTrace();  //e:
    } catch (IOException e) {
      e.printStackTrace();  //e:
    }
  }

  public static void main(String[] args) {
    
    List<LogicalOperator> ops = new ArrayList<>();
  }
}
