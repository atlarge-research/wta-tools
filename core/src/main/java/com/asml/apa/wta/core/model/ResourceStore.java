package com.asml.apa.wta.core.model;

import java.util.List;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Wrapper for a {@link java.util.List} of {@link Resource}s.
 * Necessary as generics are not part of the type erasure.
 * This means that otherwise we cannot use overloaded methods for
 * {@link List}s of {@link Resource}s and {@link Task}s.
 *
 * @author Atour Mousavi Gourabi
 * @since 1.0.0
 */
@RequiredArgsConstructor
public class ResourceStore {

  @Getter
  private final List<Resource> resources;
}
