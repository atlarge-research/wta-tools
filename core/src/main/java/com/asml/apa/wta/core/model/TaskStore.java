package com.asml.apa.wta.core.model;

import java.util.List;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Wrapper for a {@link List} of {@link Task}s.
 * Necessary as generics are not part of the type erasure.
 * This means that otherwise we cannot use overloaded methods for
 * {@link List}s of {@link Task}s and {@link Workflow}s.
 *
 * @author Atour Mousavi Gourabi
 * @since 1.0.0
 */
@RequiredArgsConstructor
public class TaskStore {

  @Getter
  private final List<Task> tasks;
}
