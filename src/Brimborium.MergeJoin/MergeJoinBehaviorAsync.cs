namespace Brimborium.MergeJoin;

/// <summary>
/// Implements an asynchronous merge-join algorithm between two collections of items.
/// This class provides the same functionality as <see cref="MergeJoinBehavior{TSource, TTarget}"/> 
/// but with async/await support for all operations.
/// </summary>
/// <typeparam name="TSource">The type of the source collection items.</typeparam>
/// <typeparam name="TTarget">The type of the target collection items.</typeparam>
public class MergeJoinBehaviorAsync<TSource, TTarget> {
    private readonly Func<TSource, TTarget, int>? _KeyComparer;
    private readonly Func<TSource, TTarget, ValueTask>? _UpdateAction;
    private readonly Func<TSource, ValueTask>? _InsertAction;
    private readonly Func<TTarget, ValueTask>? _DeleteAction;

    /// <summary>
    /// Initializes a new instance of the <see cref="MergeJoinBehaviorAsync{TSource, TTarget}"/> class.
    /// </summary>
    /// <param name="keyComparer">Function to compare source and target items.</param>
    /// <param name="updateAction">Action to perform when items match.</param>
    /// <param name="insertAction">Action to perform when a source item needs to be inserted.</param>
    /// <param name="deleteAction">Action to perform when a target item needs to be deleted.</param>
    public MergeJoinBehaviorAsync(
        Func<TSource, TTarget, int>? keyComparer = default,
        Func<TSource, TTarget, ValueTask>? updateAction = default,
        Func<TSource, ValueTask>? insertAction = default,
        Func<TTarget, ValueTask>? deleteAction = default
    ) {
        this._KeyComparer = keyComparer;
        this._UpdateAction = updateAction;
        this._InsertAction = insertAction;
        this._DeleteAction = deleteAction;
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

    /// <summary>
    /// Updates a target item with data from a source item when they match.
    /// </summary>
    /// <param name="source">The source item.</param>
    /// <param name="target">The target item to update.</param>
    protected virtual async Task UpdateAction(TSource source, TTarget target) {
        if (this._UpdateAction is null) {
            // No update action specified, do nothing
        } else {
            await this._UpdateAction(source, target);
        }
    }

    /// <summary>
    /// Handles insertion of a source item that doesn't exist in the target.
    /// </summary>
    /// <param name="source">The source item to insert.</param>
    protected virtual async Task InsertAction(TSource source) {
        if (this._InsertAction is null) {
            // No insert action specified, do nothing
        } else {
            await this._InsertAction(source);
        }
    }

    /// <summary>
    /// Handles deletion of a target item that doesn't exist in the source.
    /// </summary>
    /// <param name="target">The target item to delete.</param>
    protected virtual async Task DeleteAction(TTarget target) {
        if (this._DeleteAction is null) {
            // No delete action specified, do nothing
        } else {
            await this._DeleteAction(target);
        }
    }

    /// <summary>
    /// Executes the merge-join algorithm on the source and target collections.
    /// </summary>
    /// <param name="listSource">The source collection.</param>
    /// <param name="listTarget">The target collection.</param>
    public async Task Execute(IEnumerable<TSource> listSource, IEnumerable<TTarget> listTarget) {
        // Get enumerators for both collections
        var sourceEnumerator = listSource.GetEnumerator();
        var targetEnumerator = listTarget.GetEnumerator();
        
        // Initialize with first items if available
        var sourceExists = sourceEnumerator.MoveNext();
        var targetExists = targetEnumerator.MoveNext();
        var sourceCurrent = sourceExists ? sourceEnumerator.Current : default;
        var targetCurrent = targetExists ? targetEnumerator.Current : default;

        // Process both collections while both have items
        while (sourceExists && targetExists) {
            var cmp = this.KeyComparer(sourceCurrent!, targetCurrent!);
            if (0 == cmp) {
                // Items match - update target with source data
                await this.UpdateAction(sourceCurrent!, targetCurrent!);
                sourceExists = sourceEnumerator.MoveNext();
                targetExists = targetEnumerator.MoveNext();
                sourceCurrent = sourceExists ? sourceEnumerator.Current : default;
                targetCurrent = targetExists ? targetEnumerator.Current : default;
            } else if (0 > cmp) {
                // Source item comes before target - insert source item
                await this.InsertAction(sourceCurrent!);
                sourceExists = sourceEnumerator.MoveNext();
                sourceCurrent = sourceExists ? sourceEnumerator.Current : default;
            } else {
                // Target item comes before source - delete target item
                await this.DeleteAction(targetCurrent!);
                targetExists = targetEnumerator.MoveNext();
                targetCurrent = targetExists ? targetEnumerator.Current : default;
            }
        }

        // Early exit if both collections are exhausted
        if (!sourceExists && !targetExists) {
            return;
        }

        // Process remaining source items (all inserts)
        if (sourceExists && !targetExists) {
            while (sourceExists) {
                await this.InsertAction(sourceCurrent!);
                sourceExists = sourceEnumerator.MoveNext();
                sourceCurrent = sourceExists ? sourceEnumerator.Current : default;
            }
            return;
        }

        // Process remaining target items (all deletes)
        if (!sourceExists && targetExists) {
            while (targetExists) {
                await this.DeleteAction(targetCurrent!);
                targetExists = targetEnumerator.MoveNext();
                targetCurrent = targetExists ? targetEnumerator.Current : default;
            }
            return;
        }
    }
}
