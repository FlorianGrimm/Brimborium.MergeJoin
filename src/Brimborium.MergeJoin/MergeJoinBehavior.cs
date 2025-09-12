using System.Net.Http.Headers;

namespace Brimborium.MergeJoin;

/// <summary>
/// Implements a merge-join algorithm between two collections of items.
/// </summary>
/// <typeparam name="TSource">The type of the source collection items.</typeparam>
/// <typeparam name="TTarget">The type of the target collection items.</typeparam>
public class MergeJoinBehavior<TSource, TTarget> {
    private readonly Func<TSource, TTarget, int>? _KeyComparer;
    private readonly Action<TSource, TTarget>? _UpdateAction;
    private readonly Action<TSource>? _InsertAction;
    private readonly Action<TTarget>? _DeleteAction;
    private readonly Func<TSource, TSource, int>? _KeyComparerSource;
    private readonly Func<TTarget, TTarget, int>? _KeyComparerTarget;

    /// <summary>
    /// Initializes a new instance of the <see cref="MergeJoinBehavior{TSource, TTarget}"/> class.
    /// </summary>
    /// <param name="keyComparer">Function to compare source and target items.</param>
    /// <param name="updateAction">Action to perform when items match.</param>
    /// <param name="insertAction">Action to perform when a source item needs to be inserted.</param>
    /// <param name="deleteAction">Action to perform when a target item needs to be deleted.</param>
    /// <param name="keyComparerSource">Function to compare source items.</param>
    /// <param name="keyComparerTarget">Function to compare target items.</param>
    public MergeJoinBehavior(
        Func<TSource, TTarget, int>? keyComparer = default,
        Action<TSource, TTarget>? updateAction = default,
        Action<TSource>? insertAction = default,
        Action<TTarget>? deleteAction = default,
        Func<TSource, TSource, int>? keyComparerSource = default,
        Func<TTarget, TTarget, int>? keyComparerTarget = default
    ) {
        this._KeyComparer = keyComparer;
        this._UpdateAction = updateAction;
        this._InsertAction = insertAction;
        this._DeleteAction = deleteAction;
        this._KeyComparerSource = keyComparerSource;
        this._KeyComparerTarget = keyComparerTarget;
    }

    /// <summary>
    /// Compares a source item with a target item.
    /// </summary>
    /// <param name="source">The source item.</param>
    /// <param name="target">The target item.</param>
    /// <returns>
    /// A negative value if source comes before target;
    /// zero if they are equal;
    /// a positive value if source comes after target.
    /// </returns>
    /// <exception cref="NotImplementedException">Thrown when no key comparer was provided.</exception>
    protected virtual int KeyComparer(TSource source, TTarget target) {
        if (this._KeyComparer is null) {
            throw new NotImplementedException();
        } else {
            return this._KeyComparer(source, target);
        }
    }

    private void SourceMoveNext(ref IEnumerator<TSource> sourceEnumerator, ref bool sourceExists, ref TSource sourceCurrent) {
        sourceExists = sourceEnumerator.MoveNext();
        if (sourceExists) {
            var sourceAfter = sourceEnumerator.Current;
            var result = this.SourceCompare(sourceCurrent, sourceAfter);
            if (result < 0) {
                sourceCurrent = sourceAfter;
            } else if (result == 0){
                throw new System.ArgumentException("Source is not unique.");
            } else {
                throw new System.ArgumentException("Source is not sorted.");
            }
        }
    }

    private void TargetMoveNext(ref IEnumerator<TTarget> targetEnumerator, ref bool targetExists, ref TTarget targetCurrent) {
        targetExists = targetEnumerator.MoveNext();
        if (targetExists) {
            var targetAfter = targetEnumerator.Current;
            var result = this.TargetCompare(targetCurrent, targetAfter);
            if (result < 0) {
                targetCurrent = targetAfter;
            } else if (result == 0){
                throw new System.ArgumentException("Target is not unique.");
            } else {
                throw new System.ArgumentException("Target is not sorted.");
            }
        }
    }

    /// <summary>
    /// Compares two source items to validate sorting order.
    /// </summary>
    /// <param name="before">The first source item.</param>
    /// <param name="after">The second source item.</param>
    /// <returns>
    /// A negative value if before comes before after;
    /// zero if they are equal;
    /// a positive value if before comes after after.
    /// Returns 0 if no source comparer is provided.
    /// </returns>
    protected virtual int SourceCompare(TSource before, TSource after) {
        if (this._KeyComparerSource is { } keyComparerSource) {
            return keyComparerSource(before, after);
        }
        return 0;
    }

    /// <summary>
    /// Compares two target items to validate sorting order.
    /// </summary>
    /// <param name="before">The first target item.</param>
    /// <param name="after">The second target item.</param>
    /// <returns>
    /// A negative value if before comes before after;
    /// zero if they are equal;
    /// a positive value if before comes after after.
    /// Returns 0 if no target comparer is provided.
    /// </returns>
    protected virtual int TargetCompare(TTarget before, TTarget after) {
        if (this._KeyComparerTarget is { } keyComparerTarget) {
            return keyComparerTarget(before, after);
        }
        return 0;
    }

    /// <summary>
    /// Updates a target item with data from a source item when they match.
    /// </summary>
    /// <param name="source">The source item.</param>
    /// <param name="target">The target item to update.</param>
    protected virtual void UpdateAction(TSource source, TTarget target) {
        if (this._UpdateAction is null) {
            // No update action specified, do nothing
        } else {
            this._UpdateAction(source, target);
        }
    }

    /// <summary>
    /// Handles insertion of a source item that doesn't exist in the target.
    /// </summary>
    /// <param name="source">The source item to insert.</param>
    protected virtual void InsertAction(TSource source) {
        if (this._InsertAction is null) {
            // No insert action specified, do nothing
        } else {
            this._InsertAction(source);
        }
    }

    /// <summary>
    /// Handles deletion of a target item that doesn't exist in the source.
    /// </summary>
    /// <param name="target">The target item to delete.</param>
    protected virtual void DeleteAction(TTarget target) {
        if (this._DeleteAction is null) {
            // No delete action specified, do nothing
        } else {
            this._DeleteAction(target);
        }
    }

    /// <summary>
    /// Executes the merge-join algorithm on the source and target collections.
    /// </summary>
    /// <param name="listSource">The source collection.</param>
    /// <param name="listTarget">The target collection.</param>
    public void Execute(IEnumerable<TSource> listSource, IEnumerable<TTarget> listTarget) {
        // Get enumerators for both collections
        var sourceEnumerator = listSource.GetEnumerator();
        var targetEnumerator = listTarget.GetEnumerator();

        // Initialize with first items if available
        var sourceExists = sourceEnumerator.MoveNext();
        var targetExists = targetEnumerator.MoveNext();

        if (sourceExists && targetExists) {

            var sourceCurrent = sourceEnumerator.Current;
            var targetCurrent = targetEnumerator.Current;

            // Process both collections while both have items
            while (sourceExists && targetExists) {
                var cmp = this.KeyComparer(sourceCurrent, targetCurrent);
                if (0 == cmp) {
                    // Items match - update target with source data
                    this.UpdateAction(sourceCurrent, targetCurrent);
                    this.SourceMoveNext(ref sourceEnumerator, ref sourceExists, ref sourceCurrent);
                    this.TargetMoveNext(ref targetEnumerator, ref targetExists, ref targetCurrent);
                } else if (0 > cmp) {
                    // Source item comes before target - insert source item
                    this.InsertAction(sourceCurrent);
                    this.SourceMoveNext(ref sourceEnumerator, ref sourceExists, ref sourceCurrent);
                } else {
                    // Target item comes before source - delete target item
                    this.DeleteAction(targetCurrent);
                    this.TargetMoveNext(ref targetEnumerator, ref targetExists, ref targetCurrent);
                }
            }
        }

        // Early exit if both collections are exhausted
        if (!sourceExists && !targetExists) {
            return;
        }

        // Process remaining source items (all inserts)
        if (sourceExists && !targetExists) {
            while (sourceExists) {
                var sourceCurrent = sourceEnumerator.Current;
                this.InsertAction(sourceCurrent);
                sourceExists = sourceEnumerator.MoveNext();
            }
            return;
        }

        // Process remaining target items (all deletes)
        if (!sourceExists && targetExists) {
            while (targetExists) {
                var targetCurrent = targetEnumerator.Current;
                this.DeleteAction(targetCurrent);
                targetExists = targetEnumerator.MoveNext();
            }
            return;
        }
    }
}
