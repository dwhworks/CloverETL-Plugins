package org.dwhworks.component;

import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.Node;

/**
 * Utility abstract class is used for creating new component.
 *
 * @author Nikita Skotnikov
 * @since 27.03.2018
 */
public abstract class AbstractComponent extends Node {

  /**
   * Constructor
   *
   * @param id component id in the graph
   */
  public AbstractComponent(String id) {
    super(id);
  }

  @Override
  public void init() throws ComponentNotReadyException {
    if (isInitialized()) return;
    super.init();
    checkGraphParameters();
    checkAttributes();
  }

  /**
   * Do checks around graph parameters before node will be executed.
   */
  protected abstract void checkGraphParameters();

  /**
   * Do checks around attributes before node will be executed.
   */
  protected abstract void checkAttributes() throws ComponentNotReadyException;


}
