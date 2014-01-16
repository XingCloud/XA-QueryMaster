package com.xingcloud.qm.utils;

import com.jgraph.layout.JGraphFacade;
import com.jgraph.layout.hierarchical.JGraphHierarchicalLayout;
import com.mxgraph.layout.hierarchical.mxHierarchicalLayout;
import com.mxgraph.util.mxCellRenderer;
import com.mxgraph.util.mxConstants;
import com.mxgraph.util.mxUtils;
import com.mxgraph.view.mxGraph;
import org.apache.drill.common.graph.AdjacencyList;
import org.apache.drill.common.graph.Edge;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.drill.common.logical.data.Scan;
import org.apache.drill.common.logical.data.UnionedScan;
import org.apache.drill.common.logical.data.UnionedScanSplit;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.jgraph.JGraph;
import org.jgraph.graph.AttributeMap;
import org.jgraph.graph.GraphConstants;
import org.jgrapht.DirectedGraph;
import org.jgrapht.Graph;
import org.jgrapht.ext.JGraphModelAdapter;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleDirectedGraph;
import org.w3c.dom.Document;

import javax.imageio.ImageIO;
import javax.swing.*;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.awt.*;
import java.awt.geom.Rectangle2D;
import java.awt.image.BufferedImage;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;

public class GraphVisualize {
  
  private static final int VERTEX_HEIGHT = 30;
  private static final int VERTEX_WIDTH = 140;
  private static final int SCAN_WIDTH = 200;
  private static final int VERTEX_FONTSIZE = 6;
  private static final String STYLE_SCAN = "scan";
  
  private static mxGraph buildDirectedGraphMX(LogicalPlan plan) {
    Collection<Edge<AdjacencyList<LogicalOperator>.Node>> edges = plan.getGraph().getAdjList().getAllEdges();    
    mxGraph mx = new mxGraph();
    mx.getModel().beginUpdate();
    initStyle(mx);
    Object parent = mx.getDefaultParent();
    Map<LogicalOperator, Object> vertexes = new HashMap<>();    
    for(Edge<AdjacencyList<LogicalOperator>.Node> edge:edges){
      LogicalOperator node = edge.getFrom().getNodeValue();
      if(!vertexes.containsKey(node)){
        Object cell = insertVertex(parent, node, mx);
        vertexes.put(node, cell);
      }
      Object source = vertexes.get(node);
      node = edge.getTo().getNodeValue();
      if(!vertexes.containsKey(node)){
        
        Object cell = insertVertex(parent, node, mx);      
        vertexes.put(node, cell);
      }
      Object target = vertexes.get(node);
      mx.insertEdge(parent, null, null, source, target);
    }
    mx.getModel().endUpdate();
    vertexes.clear();
    return mx;
  }

  private static void initStyle(mxGraph mx) {
    //style
    Map<String, Object> scanStyle = new HashMap<>();
    String color = "#"+mxUtils.getHexColorString(new Color(255, 195, 217, 0));
    scanStyle.put(mxConstants.STYLE_FILLCOLOR, color);//
    mx.getStylesheet().putCellStyle(STYLE_SCAN, scanStyle);
  }

  private static Object insertVertex(Object parent, LogicalOperator operator, mxGraph mx) {
    String style = null;
    if(operator instanceof Scan || operator instanceof UnionedScan || operator instanceof UnionedScanSplit){
      style = STYLE_SCAN;
//      style = null;
    }
    return mx.insertVertex(parent, null, operator, 0, 0, VERTEX_WIDTH, VERTEX_HEIGHT, style);
  }


  static int WIDTH = 1200;
  static int HEIGHT = 1200;
  
  public static void visualizeMX(LogicalPlan plan, String svgPath){
    try {
      mxGraph graph = buildDirectedGraphMX(plan);
      mxHierarchicalLayout hierarchical = new mxHierarchicalLayout(graph);
//      mxCompactTreeLayout tree =  new mxCompactTreeLayout(graph, true);
//      tree.setEdgeRouting(true);
//      tree.execute(graph.getDefaultParent());
      hierarchical.setInterRankCellSpacing(100.0);
      hierarchical.execute(graph.getDefaultParent());
      Document svg = mxCellRenderer.createSvgDocument(graph, null, 1, Color.WHITE, null);
      TransformerFactory tFactory = TransformerFactory.newInstance();
      Transformer transformer = tFactory.newTransformer();
      
      DOMSource source = new DOMSource(svg);
      StreamResult result = new StreamResult(new FileOutputStream(svgPath));
      transformer.transform(source, result); 
      
    }catch(Exception e){
      e.printStackTrace();  //e:
    }
  }
  
  private static DirectedGraph<LogicalOperator, DefaultEdge> buildDirectedGraph(LogicalPlan plan) {
    DirectedGraph<LogicalOperator, DefaultEdge> graph = new SimpleDirectedGraph<LogicalOperator, DefaultEdge>(DefaultEdge.class);
    Collection<Edge<AdjacencyList<LogicalOperator>.Node>> edges = plan.getGraph().getAdjList().getAllEdges();
    Set<PhysicalOperator> vertexes = new HashSet<>();
    for(Edge<AdjacencyList<LogicalOperator>.Node> edge:edges){
      graph.addVertex(edge.getFrom().getNodeValue());
      graph.addVertex(edge.getTo().getNodeValue());
      graph.addEdge(edge.getFrom().getNodeValue(), edge.getTo().getNodeValue());
    }
    
    return graph;
    
  }
  public static void visualize(LogicalPlan plan, String pngPath){
    DirectedGraph<LogicalOperator, DefaultEdge> grapht = buildDirectedGraph(plan);
    // create a visualization using JGraph, via the adapter
    JGraphModelAdapter<LogicalOperator, DefaultEdge> adapter = new JGraphModelAdapter<LogicalOperator, DefaultEdge>(grapht, 
      createDefaultVertexAttributes(),
      createDefaultEdgeAttributes(grapht));
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
    
//    treeLayout.run(jgf);
//    JGraphTreeLayout treeLayout = new JGraphTreeLayout();
    JGraphHierarchicalLayout hLayout = new JGraphHierarchicalLayout();
    hLayout.setIntraCellSpacing(75);
    int spacing = (grapht.vertexSet().size()/10 + 1)*25;
    hLayout.setInterRankCellSpacing(80);
    hLayout.run(jgf);
//    treeLayout.setLevelDistance(spacing);
//    treeLayout.run(jgf);
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

  
  private static AttributeMap createDefaultVertexAttributes()
  {
      AttributeMap map = new AttributeMap();
      Color c = Color.decode("#FF9900");

      GraphConstants.setBounds(map, new Rectangle2D.Double(20, 20, 140, 30));
      GraphConstants.setBorder(map, BorderFactory.createRaisedBevelBorder());
      GraphConstants.setBackground(map, c);
      GraphConstants.setForeground(map, Color.white);
      GraphConstants.setFont(
          map,
          GraphConstants.DEFAULTFONT.deriveFont(Font.PLAIN, 6));
      GraphConstants.setOpaque(map, true);

      return map;
  }
  
  private static <V, E> AttributeMap createDefaultEdgeAttributes(
      Graph<V, E> jGraphTGraph)
  {
      AttributeMap map = new AttributeMap();

      if (jGraphTGraph instanceof DirectedGraph<?, ?>) {
          GraphConstants.setLineEnd(map, GraphConstants.ARROW_TECHNICAL);
          GraphConstants.setEndFill(map, true);
          GraphConstants.setEndSize(map, 10);
      }

      GraphConstants.setForeground(map, Color.decode("#25507C"));
      GraphConstants.setFont(
          map,
          GraphConstants.DEFAULTFONT.deriveFont(Font.PLAIN, 0));
      GraphConstants.setLineColor(map, Color.decode("#7AA1E6"));

      return map;
  }
}
